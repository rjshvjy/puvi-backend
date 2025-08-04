from datetime import datetime, date, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from db_utils import get_db_connection, close_connection
from inventory_utils import update_inventory
from decimal import Decimal, InvalidOperation

app = Flask(__name__)
CORS(app)

def safe_decimal(value, default=0):
    """
    Safely convert a value to Decimal, handling various edge cases
    
    Args:
        value: The value to convert (can be string, number, None, etc.)
        default: Default value if conversion fails (default: 0)
    
    Returns:
        Decimal: The converted value or default
    """
    try:
        # Handle None, empty string, or string with only whitespace
        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
            return Decimal(str(default))
        
        # Convert to string first to handle various numeric types
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation) as e:
        print(f"Warning: Could not convert '{value}' to Decimal. Using default: {default}. Error: {e}")
        return Decimal(str(default))

def safe_float(value, default=0):
    """
    Safely convert a value to float, handling various edge cases
    
    Args:
        value: The value to convert
        default: Default value if conversion fails
    
    Returns:
        float: The converted value or default
    """
    try:
        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
            return float(default)
        return float(value)
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not convert '{value}' to float. Using default: {default}. Error: {e}")
        return float(default)

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
        cur.execute("BEGIN")
        
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

# ===== BATCH PRODUCTION ENDPOINTS =====

@app.route('/api/seeds_for_batch', methods=['GET'])
def get_seeds_for_batch():
    """Get available seeds from inventory for batch production"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                i.inventory_id,
                i.material_id,
                m.material_name,
                m.unit,
                i.closing_stock as available_quantity,
                i.weighted_avg_cost,
                m.category
            FROM inventory i
            JOIN materials m ON i.material_id = m.material_id
            WHERE m.category = 'Seeds' 
                AND i.closing_stock > 0
            ORDER BY m.material_name
        """)
        
        seeds = []
        for row in cur.fetchall():
            seeds.append({
                'inventory_id': row[0],
                'material_id': row[1],
                'material_name': row[2],
                'unit': row[3],
                'available_quantity': float(row[4]),
                'weighted_avg_cost': float(row[5]),
                'category': row[6]
            })
        
        return jsonify({
            'success': True,
            'seeds': seeds
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/cost_elements_for_batch', methods=['GET'])
def get_cost_elements_for_batch():
    """Get applicable cost elements for batch production"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get cost elements relevant for batch production
        cur.execute("""
            SELECT 
                element_id,
                element_name,
                category,
                unit_type,
                default_rate,
                calculation_method
            FROM cost_elements
            WHERE category IN ('Labor', 'Utilities', 'Maintenance')
                AND element_name IN (
                    'Drying Labour',
                    'Seed Unloading',
                    'Loading After Drying',
                    'Crushing Labour',
                    'Filtering Labour',
                    'Electricity - Crushing',
                    'Machine Maintenance'
                )
            ORDER BY category, element_name
        """)
        
        cost_elements = []
        for row in cur.fetchall():
            cost_elements.append({
                'element_id': row[0],
                'element_name': row[1],
                'category': row[2],
                'unit_type': row[3],
                'default_rate': float(row[4]),
                'calculation_method': row[5]
            })
        
        return jsonify({
            'success': True,
            'cost_elements': cost_elements
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/oil_cake_rates', methods=['GET'])
def get_oil_cake_rates():
    """Get current oil cake rates for estimation"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get oil cake rates - you may need to create this table
        # For now, returning some default values
        oil_cake_rates = {
            'Groundnut': {'cake_rate': 30.00, 'sludge_rate': 10.00},
            'Sesame': {'cake_rate': 35.00, 'sludge_rate': 12.00},
            'Coconut': {'cake_rate': 25.00, 'sludge_rate': 8.00},
            'Mustard': {'cake_rate': 28.00, 'sludge_rate': 9.00}
        }
        
        return jsonify({
            'success': True,
            'rates': oil_cake_rates
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/add_batch', methods=['POST'])
def add_batch():
    """Create a new batch production record with comprehensive validation"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Debug logging
        print(f"Received batch data: {data}")
        
        # Parse date
        production_date = parse_date(data['production_date'])
        
        # Generate batch code
        date_str = data['production_date'].replace('-', '')  # DDMMYYYY format
        batch_code = f"BATCH-{date_str}-{data['batch_description']}"
        
        # Safely convert values to Decimal with validation
        seed_qty_before = safe_decimal(data.get('seed_quantity_before_drying', 0))
        seed_qty_after = safe_decimal(data.get('seed_quantity_after_drying', 0))
        oil_yield = safe_decimal(data.get('oil_yield', 0))
        cake_yield = safe_decimal(data.get('cake_yield', 0))
        sludge_yield = safe_decimal(data.get('sludge_yield', 0))
        
        # Validate quantities
        if seed_qty_before <= 0:
            return jsonify({'error': 'Seed quantity before drying must be greater than 0'}), 400
        if seed_qty_after <= 0:
            return jsonify({'error': 'Seed quantity after drying must be greater than 0'}), 400
        if seed_qty_after > seed_qty_before:
            return jsonify({'error': 'Seed quantity after drying cannot exceed quantity before drying'}), 400
        
        drying_loss = seed_qty_before - seed_qty_after
        
        # Calculate percentages
        oil_yield_percent = (oil_yield / seed_qty_after * 100) if seed_qty_after > 0 else 0
        cake_yield_percent = (cake_yield / seed_qty_after * 100) if seed_qty_after > 0 else 0
        sludge_yield_percent = (sludge_yield / seed_qty_after * 100) if seed_qty_after > 0 else 0
        
        # Begin transaction
        cur.execute("BEGIN")
        
        # Insert batch record with additional fields
        cur.execute("""
            INSERT INTO batch (
                batch_code, oil_type, seed_quantity_before_drying,
                seed_quantity_after_drying, drying_loss, oil_yield,
                oil_yield_percent, oil_cake_yield, oil_cake_yield_percent,
                sludge_yield, sludge_yield_percent, production_date, recipe_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING batch_id
        """, (
            batch_code,
            data['oil_type'],
            float(seed_qty_before),
            float(seed_qty_after),
            float(drying_loss),
            float(oil_yield),
            float(oil_yield_percent),
            float(cake_yield),
            float(cake_yield_percent),
            float(sludge_yield),
            float(sludge_yield_percent),
            production_date,
            None  # recipe_id - can be added later
        ))
        
        batch_id = cur.fetchone()[0]
        
        # Process cost details
        total_production_cost = safe_decimal(data.get('seed_cost_total', 0))
        
        # Insert all cost elements with validation
        cost_details = data.get('cost_details', [])
        for cost_item in cost_details:
            # Validate each cost item
            element_name = cost_item.get('element_name', '')
            master_rate = safe_float(cost_item.get('master_rate', 0))
            
            # Handle override rate - if empty string or None, use master rate
            override_rate_value = cost_item.get('override_rate')
            if override_rate_value in (None, '', 'null'):
                override_rate = master_rate
            else:
                override_rate = safe_float(override_rate_value, master_rate)
            
            quantity = safe_float(cost_item.get('quantity', 0))
            total_cost = safe_float(cost_item.get('total_cost', 0))
            
            # Add to total production cost
            total_production_cost += Decimal(str(total_cost))
            
            cur.execute("""
                INSERT INTO batch_cost_details (
                    batch_id, cost_element, master_rate, 
                    override_rate, quantity, total_cost
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                element_name,
                master_rate,
                override_rate,
                quantity,
                total_cost
            ))
        
        # Calculate net oil cost with safe decimal conversion
        cake_estimated_rate = safe_decimal(data.get('cake_estimated_rate', 0))
        sludge_estimated_rate = safe_decimal(data.get('sludge_estimated_rate', 0))
        
        cake_revenue = cake_yield * cake_estimated_rate
        sludge_revenue = sludge_yield * sludge_estimated_rate
        net_oil_cost = total_production_cost - cake_revenue - sludge_revenue
        oil_cost_per_kg = net_oil_cost / oil_yield if oil_yield > 0 else 0
        
        # Update batch with cost information
        cur.execute("""
            UPDATE batch 
            SET total_production_cost = %s,
                net_oil_cost = %s,
                oil_cost_per_kg = %s,
                cake_estimated_rate = %s,
                sludge_estimated_rate = %s
            WHERE batch_id = %s
        """, (
            float(total_production_cost),
            float(net_oil_cost),
            float(oil_cost_per_kg),
            float(cake_estimated_rate),
            float(sludge_estimated_rate),
            batch_id
        ))
        
        # Update inventory
        # 1. Reduce seed inventory
        cur.execute("""
            UPDATE inventory
            SET closing_stock = closing_stock - %s,
                consumption = consumption + %s,
                last_updated = %s
            WHERE material_id = %s
        """, (
            float(seed_qty_before),
            float(seed_qty_before),
            production_date,
            data['material_id']
        ))
        
        # 2. Add oil to inventory
        # Check if oil inventory exists
        cur.execute("""
            SELECT inventory_id FROM inventory 
            WHERE material_id IS NULL 
                AND product_id IS NULL
                AND oil_type = %s
                AND source_type = 'extraction'
        """, (data['oil_type'],))
        
        oil_inv = cur.fetchone()
        
        if oil_inv:
            # Update existing oil inventory
            cur.execute("""
                UPDATE inventory
                SET closing_stock = closing_stock + %s,
                    last_updated = %s
                WHERE inventory_id = %s
            """, (float(oil_yield), production_date, oil_inv[0]))
        else:
            # Create new oil inventory record
            cur.execute("""
                INSERT INTO inventory (
                    oil_type, closing_stock, weighted_avg_cost,
                    last_updated, source_type, source_reference_id,
                    is_bulk_oil
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                data['oil_type'],
                float(oil_yield),
                float(oil_cost_per_kg),
                production_date,
                'extraction',
                batch_id,
                True
            ))
        
        # 3. Add oil cake to inventory
        if cake_yield > 0:
            cur.execute("""
                INSERT INTO oil_cake_inventory (
                    batch_id, oil_type, quantity_produced,
                    quantity_remaining, estimated_rate, production_date
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                data['oil_type'],
                float(cake_yield),
                float(cake_yield),
                float(cake_estimated_rate),
                production_date
            ))
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'batch_id': batch_id,
            'batch_code': batch_code,
            'oil_cost_per_kg': float(oil_cost_per_kg),
            'total_oil_produced': float(oil_yield),
            'message': f'Batch {batch_code} created successfully!'
        }), 201
        
    except Exception as e:
        conn.rollback()
        print(f"Error in add_batch: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/batch_history', methods=['GET'])
def get_batch_history():
    """Get batch production history"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        limit = request.args.get('limit', 50, type=int)
        oil_type = request.args.get('oil_type', None)
        
        query = """
            SELECT 
                b.batch_id,
                b.batch_code,
                b.oil_type,
                b.production_date,
                b.seed_quantity_after_drying,
                b.oil_yield,
                b.oil_yield_percent,
                b.oil_cake_yield,
                b.oil_cost_per_kg,
                b.net_oil_cost,
                b.total_production_cost,
                b.sludge_yield,
                b.cake_estimated_rate
            FROM batch b
        """
        
        params = []
        if oil_type:
            query += " WHERE b.oil_type = %s"
            params.append(oil_type)
            
        query += " ORDER BY b.production_date DESC, b.batch_id DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        
        batches = []
        for row in cur.fetchall():
            batches.append({
                'batch_id': row[0],
                'batch_code': row[1],
                'oil_type': row[2],
                'production_date': integer_to_date(row[3]),
                'seed_quantity': float(row[4]),
                'oil_yield': float(row[5]),
                'oil_yield_percent': float(row[6]),
                'cake_yield': float(row[7]),
                'oil_cost_per_kg': float(row[8]),
                'net_oil_cost': float(row[9]),
                'total_production_cost': float(row[10]),
                'sludge_yield': float(row[11]) if row[11] else 0,
                'cake_rate': float(row[12]) if row[12] else 0
            })
        
        # Get summary statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_batches,
                SUM(oil_yield) as total_oil_produced,
                SUM(oil_cake_yield) as total_cake_produced,
                AVG(oil_yield_percent) as avg_oil_yield_percent,
                AVG(oil_cost_per_kg) as avg_oil_cost
            FROM batch
        """)
        
        stats = cur.fetchone()
        
        return jsonify({
            'success': True,
            'batches': batches,
            'summary': {
                'total_batches': stats[0],
                'total_oil_produced': float(stats[1] or 0),
                'total_cake_produced': float(stats[2] or 0),
                'avg_oil_yield_percent': float(stats[3] or 0),
                'avg_oil_cost': float(stats[4] or 0)
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
        'version': '3.1'  # Updated version
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
        cur.execute("SELECT COUNT(*) FROM batch")
        batch_count = cur.fetchone()[0]
        close_connection(conn, cur)
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'materials_count': material_count,
            'purchases_count': purchase_count,
            'batches_count': batch_count,
            'version': '3.1'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database': 'disconnected',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
