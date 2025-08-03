from datetime import datetime, date, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from db_utils import get_db_connection, close_connection
from inventory_utils import update_inventory
from decimal import Decimal

app = Flask(__name__)
CORS(app)

def date_to_day_number(date_string):
    """Convert date string (DD-MM-YYYY) to day number since epoch"""
    # Handle both formats for compatibility
    if '-' in date_string and len(date_string.split('-')[0]) == 4:
        # YYYY-MM-DD format from HTML date input
        date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    else:
        # DD-MM-YYYY format
        date_obj = datetime.strptime(date_string, '%d-%m-%Y')
    epoch = datetime(1970, 1, 1)
    return (date_obj - epoch).days

def parse_date(date_string):
    """
    Parse date from various formats to integer (days since epoch)
    Accepts: YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY
    """
    if not date_string:
        return None
    
    # Try different date formats
    formats = [
        '%Y-%m-%d',  # ISO format (from HTML date inputs)
        '%d-%m-%Y',  # Indian format with dash
        '%d/%m/%Y',  # Indian format with slash
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(str(date_string), fmt).date()
            # Convert to days since epoch (1970-01-01)
            epoch = date(1970, 1, 1)
            return (dt - epoch).days
        except ValueError:
            continue
    
    # If date_string is already an integer, return it
    try:
        return int(date_string)
    except ValueError:
        raise ValueError(f"Unable to parse date: {date_string}")

def integer_to_date(days_since_epoch):
    """
    Convert integer (days since epoch) to DD-MM-YYYY format string
    """
    if days_since_epoch is None:
        return ''
    
    try:
        # Convert integer to date
        epoch = date(1970, 1, 1)
        dt = epoch + timedelta(days=int(days_since_epoch))
        # Format as DD-MM-YYYY
        return dt.strftime('%d-%m-%Y')
    except:
        return ''

@app.route('/api/add_purchase', methods=['POST'])
def add_purchase():
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Convert string values to Decimal for accurate calculation
        quantity = Decimal(str(data['quantity']))
        cost_per_unit = Decimal(str(data['cost_per_unit']))
        gst_rate = Decimal(str(data['gst_rate']))
        transport_cost = Decimal(str(data.get('transport_cost', 0)))
        loading_charges = Decimal(str(data.get('loading_charges', 0)))
        
        # Calculate costs
        material_cost = quantity * cost_per_unit
        subtotal = material_cost + transport_cost + loading_charges
        gst_amount = subtotal * (gst_rate / 100)
        total_cost = subtotal + gst_amount
        landed_cost_per_unit = total_cost / quantity if quantity > 0 else 0
        
        # Convert date to day number
        purchase_date_day = date_to_day_number(data['purchase_date'])
        
        # Insert purchase record with all new fields
        cur.execute("""
            INSERT INTO purchases (
                material_id, quantity, cost_per_unit, gst_rate, 
                invoice_ref, purchase_date, supplier_name, batch_number,
                transport_cost, loading_charges, total_cost, landed_cost_per_unit
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['material_id'], 
            float(quantity), 
            float(cost_per_unit), 
            float(gst_rate), 
            data['invoice_ref'], 
            purchase_date_day,
            data.get('supplier_name', ''),
            data.get('batch_number', ''),
            float(transport_cost),
            float(loading_charges),
            float(total_cost),
            float(landed_cost_per_unit)
        ))
        
        # Update inventory with landed cost instead of basic cost
        update_inventory(
            data['material_id'], 
            float(quantity), 
            float(landed_cost_per_unit),  # Use landed cost for weighted average
            conn, 
            cur
        )
        
        conn.commit()
        
        # Get updated weighted average cost
        cur.execute(
            "SELECT weighted_avg_cost FROM inventory WHERE material_id = %s", 
            (data['material_id'],)
        )
        result = cur.fetchone()
        new_avg = result[0] if result else landed_cost_per_unit
        
        # Update materials table with new weighted avg
        cur.execute(
            "UPDATE materials SET current_cost = %s, last_updated = %s WHERE material_id = %s", 
            (float(new_avg), purchase_date_day, data['material_id'])
        )
        conn.commit()
        
        return jsonify({
            'message': 'Purchase added successfully',
            'new_weighted_avg': float(new_avg),
            'landed_cost_per_unit': float(landed_cost_per_unit),
            'total_cost': float(total_cost)
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        close_connection(conn, cur)

@app.route('/api/quality_check', methods=['POST'])
def add_quality_check():
    """
    Record quality check results and update inventory accordingly
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['purchase_id', 'moisture_percent', 'foreign_matter_percent', 
                          'oil_content_percent', 'status', 'checked_date']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Parse the date
        checked_date_int = parse_date(data['checked_date'])
        
        # Get purchase details first
        purchase_query = """
            SELECT p.*, m.material_name, m.unit 
            FROM purchases p
            JOIN materials m ON p.material_id = m.material_id
            WHERE p.purchase_id = %s
        """
        cur.execute(purchase_query, (data['purchase_id'],))
        purchase = cur.fetchone()
        
        if not purchase:
            return jsonify({'error': 'Purchase not found'}), 404
        
        # Convert to dictionary
        purchase_dict = {
            'purchase_id': purchase[0],
            'material_id': purchase[1],
            'quantity': purchase[2],
            'cost_per_unit': purchase[3],
            'gst_rate': purchase[4],
            'invoice_ref': purchase[5],
            'purchase_date': purchase[6],
            'supplier_name': purchase[7],
            'batch_number': purchase[8],
            'transport_cost': purchase[9],
            'loading_charges': purchase[10],
            'total_cost': purchase[11],
            'landed_cost_per_unit': purchase[12],
            'material_name': purchase[13],
            'unit': purchase[14]
        }
        
        # Calculate accepted and rejected quantities
        total_quantity = float(purchase_dict['quantity'])
        
        if data['status'] == 'Pass':
            accepted_quantity = total_quantity
            rejection_quantity = 0
        elif data['status'] == 'Reject':
            accepted_quantity = 0
            rejection_quantity = total_quantity
        else:  # Conditional - calculate based on quality parameters
            # You can implement custom logic here based on quality thresholds
            # For now, let's accept 90% if conditional
            rejection_percent = min(
                max(float(data.get('moisture_percent', 0)) - 12, 0) * 2 +  # Excess moisture
                float(data.get('foreign_matter_percent', 0)) * 3,  # Foreign matter impact
                50  # Max 50% rejection
            )
            rejection_quantity = total_quantity * (rejection_percent / 100)
            accepted_quantity = total_quantity - rejection_quantity
        
        # Begin transaction
        conn.execute("BEGIN")
        
        # Insert quality check record
        qc_insert = """
            INSERT INTO quality_checks (
                purchase_id, moisture_percent, foreign_matter_percent, 
                oil_content_percent, status, checked_date, checked_by,
                rejection_quantity, accepted_quantity, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING qc_id
        """
        
        cur.execute(qc_insert, (
            data['purchase_id'],
            data['moisture_percent'],
            data['foreign_matter_percent'],
            data['oil_content_percent'],
            data['status'],
            checked_date_int,
            data.get('checked_by', 'System'),
            rejection_quantity,
            accepted_quantity,
            data.get('notes', '')
        ))
        
        qc_id = cur.fetchone()[0]
        
        # Update inventory based on QC results
        if accepted_quantity > 0:
            # Get current inventory
            inv_query = """
                SELECT * FROM inventory 
                WHERE material_id = %s
                ORDER BY inventory_id DESC
                LIMIT 1
            """
            cur.execute(inv_query, (purchase_dict['material_id'],))
            current_inv = cur.fetchone()
            
            if current_inv:
                old_stock = float(current_inv[4])  # closing_stock
                old_avg_cost = float(current_inv[5])  # weighted_avg_cost
            else:
                old_stock = 0
                old_avg_cost = 0
            
            # Calculate new weighted average cost
            # Use the landed cost from purchase
            new_cost_per_unit = float(purchase_dict['landed_cost_per_unit'])
            
            # Weighted average: (old_stock * old_avg + new_qty * new_cost) / (old_stock + new_qty)
            if old_stock + accepted_quantity > 0:
                new_weighted_avg = (
                    (old_stock * old_avg_cost + accepted_quantity * new_cost_per_unit) / 
                    (old_stock + accepted_quantity)
                )
            else:
                new_weighted_avg = new_cost_per_unit
            
            # Insert new inventory record
            inv_insert = """
                INSERT INTO inventory (
                    material_id, opening_stock, purchases, consumption, 
                    closing_stock, weighted_avg_cost, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cur.execute(inv_insert, (
                purchase_dict['material_id'],
                old_stock,
                accepted_quantity,  # Only accepted quantity goes to inventory
                0,
                old_stock + accepted_quantity,
                new_weighted_avg,
                checked_date_int
            ))
            
            # Update material current cost
            mat_update = """
                UPDATE materials 
                SET current_cost = %s, last_updated = %s
                WHERE material_id = %s
            """
            cur.execute(mat_update, (
                new_weighted_avg,
                checked_date_int,
                purchase_dict['material_id']
            ))
        
        # If there are rejections, optionally create an inventory adjustment record
        if rejection_quantity > 0:
            # This will be handled by the inventory adjustments module
            # For now, we just track it in the quality_checks table
            pass
        
        # Commit transaction
        conn.commit()
        
        # Return success response with details
        return jsonify({
            'success': True,
            'qc_id': qc_id,
            'purchase_id': data['purchase_id'],
            'material_name': purchase_dict['material_name'],
            'total_quantity': total_quantity,
            'accepted_quantity': accepted_quantity,
            'rejection_quantity': rejection_quantity,
            'status': data['status'],
            'message': f'Quality check recorded successfully. Accepted: {accepted_quantity:.2f} {purchase_dict["unit"]}'
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/pending_quality_checks', methods=['GET'])
def get_pending_quality_checks():
    """
    Get list of purchases that don't have quality checks yet
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        query = """
            SELECT 
                p.purchase_id,
                p.purchase_date,
                p.supplier_name,
                p.batch_number,
                p.quantity,
                p.total_cost,
                m.material_name,
                m.unit,
                m.category
            FROM purchases p
            JOIN materials m ON p.material_id = m.material_id
            LEFT JOIN quality_checks qc ON p.purchase_id = qc.purchase_id
            WHERE qc.qc_id IS NULL
            AND m.category = 'Seeds'  -- Only check quality for seeds
            ORDER BY p.purchase_date DESC
        """
        
        cur.execute(query)
        purchases = []
        
        for row in cur.fetchall():
            purchase = {
                'purchase_id': row[0],
                'purchase_date': row[1],
                'supplier_name': row[2],
                'batch_number': row[3],
                'quantity': float(row[4]),
                'total_cost': float(row[5]),
                'material_name': row[6],
                'unit': row[7],
                'category': row[8]
            }
            # Convert date integer to DD-MM-YYYY format
            if purchase['purchase_date']:
                purchase['purchase_date_display'] = integer_to_date(purchase['purchase_date'])
            purchases.append(purchase)
        
        return jsonify({
            'success': True,
            'pending_purchases': purchases,
            'count': len(purchases)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/quality_check_history', methods=['GET'])
def get_quality_check_history():
    """
    Get history of all quality checks
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        query = """
            SELECT 
                qc.*,
                p.purchase_date,
                p.supplier_name,
                p.batch_number,
                p.quantity as purchase_quantity,
                m.material_name,
                m.unit
            FROM quality_checks qc
            JOIN purchases p ON qc.purchase_id = p.purchase_id
            JOIN materials m ON p.material_id = m.material_id
            ORDER BY qc.checked_date DESC, qc.qc_id DESC
            LIMIT 100
        """
        
        cur.execute(query)
        checks = []
        
        for row in cur.fetchall():
            check = {
                'qc_id': row[0],
                'purchase_id': row[1],
                'moisture_percent': float(row[2]) if row[2] else 0,
                'foreign_matter_percent': float(row[3]) if row[3] else 0,
                'oil_content_percent': float(row[4]) if row[4] else 0,
                'status': row[5],
                'checked_date': row[6],
                'checked_by': row[7],
                'rejection_quantity': float(row[8]) if row[8] else 0,
                'accepted_quantity': float(row[9]) if row[9] else 0,
                'notes': row[10],
                'created_at': row[11],
                'purchase_date': row[12],
                'supplier_name': row[13],
                'batch_number': row[14],
                'purchase_quantity': float(row[15]) if row[15] else 0,
                'material_name': row[16],
                'unit': row[17]
            }
            
            # Convert dates
            if check['checked_date']:
                check['checked_date_display'] = integer_to_date(check['checked_date'])
            if check['purchase_date']:
                check['purchase_date_display'] = integer_to_date(check['purchase_date'])
            
            # Calculate acceptance rate
            if check['purchase_quantity'] > 0:
                check['acceptance_rate'] = (check['accepted_quantity'] / check['purchase_quantity']) * 100
            else:
                check['acceptance_rate'] = 0
                
            checks.append(check)
        
        return jsonify({
            'success': True,
            'quality_checks': checks,
            'count': len(checks)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/materials', methods=['GET'])
def get_materials():
    """Get all materials for dropdown"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT material_id, material_name, current_cost, gst_rate, unit 
            FROM materials 
            ORDER BY material_name
        """)
        
        materials = []
        for row in cur.fetchall():
            materials.append({
                'material_id': row[0],
                'material_name': row[1],
                'current_cost': float(row[2]),
                'gst_rate': float(row[3]),
                'unit': row[4]
            })
        
        return jsonify(materials)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        close_connection(conn, cur)

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        'status': 'Backend is running!', 
        'timestamp': datetime.now().isoformat(),
        'version': '2.0'  # Updated version
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM materials")
        material_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM purchases")
        purchase_count = cur.fetchone()[0]
        close_connection(conn, cur)
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'materials_count': material_count,
            'purchases_count': purchase_count
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database': 'disconnected',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
