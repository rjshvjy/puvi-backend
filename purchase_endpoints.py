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

# ===== NEW MATERIAL WRITEOFF ENDPOINTS =====

@app.route('/api/writeoff_reasons', methods=['GET'])
def get_writeoff_reasons():
    """Get all writeoff reason codes"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT reason_code, reason_description, category 
            FROM writeoff_reasons 
            ORDER BY category, reason_description
        """)
        
        reasons = []
        for row in cur.fetchall():
            reasons.append({
                'reason_code': row[0],
                'reason_description': row[1],
                'category': row[2]
            })
        
        return jsonify(reasons)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        close_connection(conn, cur)

@app.route('/api/inventory_for_writeoff', methods=['GET'])
def get_inventory_for_writeoff():
    """Get materials with current inventory for writeoff selection"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                i.inventory_id,
                i.material_id,
                m.material_name,
                m.unit,
                m.category,
                i.closing_stock,
                i.weighted_avg_cost,
                i.last_updated
            FROM inventory i
            JOIN materials m ON i.material_id = m.material_id
            WHERE i.closing_stock > 0
            ORDER BY m.material_name
        """)
        
        inventory_items = []
        for row in cur.fetchall():
            inventory_items.append({
                'inventory_id': row[0],
                'material_id': row[1],
                'material_name': row[2],
                'unit': row[3],
                'category': row[4],
                'available_quantity': float(row[5]),
                'weighted_avg_cost': float(row[6]),
                'last_updated': integer_to_date(row[7]) if row[7] else ''
            })
        
        return jsonify({
            'success': True,
            'inventory_items': inventory_items,
            'count': len(inventory_items)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/add_writeoff', methods=['POST'])
def add_writeoff():
    """Record a material writeoff"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['material_id', 'quantity', 'writeoff_date', 'reason_code']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Parse the date
        writeoff_date_int = parse_date(data['writeoff_date'])
        
        # Get current inventory and cost
        cur.execute("""
            SELECT i.closing_stock, i.weighted_avg_cost, m.material_name, m.unit
            FROM inventory i
            JOIN materials m ON i.material_id = m.material_id
            WHERE i.material_id = %s
            ORDER BY i.inventory_id DESC
            LIMIT 1
        """, (data['material_id'],))
        
        inv_row = cur.fetchone()
        if not inv_row:
            return jsonify({'error': 'Material not found in inventory'}), 404
        
        current_stock = float(inv_row[0])
        weighted_avg_cost = float(inv_row[1])
        material_name = inv_row[2]
        unit = inv_row[3]
        
        # Validate quantity
        writeoff_qty = float(data['quantity'])
        if writeoff_qty > current_stock:
            return jsonify({
                'error': f'Insufficient stock. Available: {current_stock} {unit}'
            }), 400
        
        # Calculate costs
        total_cost = writeoff_qty * weighted_avg_cost
        scrap_value = float(data.get('scrap_value', 0))
        net_loss = total_cost - scrap_value
        
        # Begin transaction
        conn.execute("BEGIN")
        
        # Insert writeoff record
        cur.execute("""
            INSERT INTO material_writeoffs (
                material_id, writeoff_date, quantity, weighted_avg_cost,
                total_cost, scrap_value, net_loss, reason_code,
                reason_description, reference_type, reference_id,
                notes, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING writeoff_id
        """, (
            data['material_id'],
            writeoff_date_int,
            writeoff_qty,
            weighted_avg_cost,
            total_cost,
            scrap_value,
            net_loss,
            data['reason_code'],
            data.get('reason_description', ''),
            data.get('reference_type', 'manual'),
            data.get('reference_id'),
            data.get('notes', ''),
            data.get('created_by', 'System')
        ))
        
        writeoff_id = cur.fetchone()[0]
        
        # Update inventory - create new record with reduced quantity
        new_closing_stock = current_stock - writeoff_qty
        
        cur.execute("""
            INSERT INTO inventory (
                material_id, opening_stock, purchases, consumption,
                closing_stock, weighted_avg_cost, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data['material_id'],
            current_stock,
            0,
            writeoff_qty,  # Record writeoff as consumption
            new_closing_stock,
            weighted_avg_cost,  # Cost remains same
            writeoff_date_int
        ))
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'writeoff_id': writeoff_id,
            'material_name': material_name,
            'quantity_written_off': writeoff_qty,
            'total_cost': total_cost,
            'scrap_value': scrap_value,
            'net_loss': net_loss,
            'new_stock_balance': new_closing_stock,
            'message': f'Writeoff recorded successfully. {writeoff_qty} {unit} written off.'
        }), 201
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/writeoff_history', methods=['GET'])
def get_writeoff_history():
    """Get writeoff history"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get limit from query params, default to 100
        limit = request.args.get('limit', 100, type=int)
        
        cur.execute("""
            SELECT 
                w.*,
                m.material_name,
                m.unit,
                m.category
            FROM material_writeoffs w
            JOIN materials m ON w.material_id = m.material_id
            ORDER BY w.writeoff_date DESC, w.writeoff_id DESC
            LIMIT %s
        """, (limit,))
        
        writeoffs = []
        for row in cur.fetchall():
            writeoff = {
                'writeoff_id': row[0],
                'material_id': row[1],
                'writeoff_date': row[2],
                'writeoff_date_display': integer_to_date(row[2]),
                'quantity': float(row[3]),
                'weighted_avg_cost': float(row[4]),
                'total_cost': float(row[5]),
                'scrap_value': float(row[6]) if row[6] else 0,
                'net_loss': float(row[7]),
                'reason_code': row[8],
                'reason_description': row[9],
                'reference_type': row[10],
                'reference_id': row[11],
                'notes': row[12],
                'created_by': row[13],
                'created_at': row[14].isoformat() if row[14] else None,
                'material_name': row[15],
                'unit': row[16],
                'category': row[17]
            }
            writeoffs.append(writeoff)
        
        # Get summary statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_writeoffs,
                COALESCE(SUM(total_cost), 0) as total_cost_sum,
                COALESCE(SUM(scrap_value), 0) as total_scrap_value,
                COALESCE(SUM(net_loss), 0) as total_net_loss
            FROM material_writeoffs
        """)
        
        stats = cur.fetchone()
        
        return jsonify({
            'success': True,
            'writeoffs': writeoffs,
            'count': len(writeoffs),
            'summary': {
                'total_writeoffs': stats[0],
                'total_cost': float(stats[1]),
                'total_scrap_recovered': float(stats[2]),
                'total_net_loss': float(stats[3])
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

# ===== EXISTING ENDPOINTS (unchanged) =====

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
