"""
Batch Production Module for PUVI Oil Manufacturing System
Handles oil extraction from seeds, cost allocation, by-product tracking, and traceability
"""

from flask import Blueprint, request, jsonify
from decimal import Decimal
from db_utils import get_db_connection, close_connection
from utils.date_utils import parse_date, integer_to_date
from utils.validation import safe_decimal, safe_float, validate_positive_number
from utils.traceability import generate_batch_traceable_code

# Create Blueprint
batch_bp = Blueprint('batch', __name__)

@batch_bp.route('/api/seeds_for_batch', methods=['GET'])
def get_seeds_for_batch():
    """Get available seeds from inventory for batch production with purchase traceable codes"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Modified query to include purchase traceable codes
        cur.execute("""
            SELECT DISTINCT ON (i.material_id)
                i.inventory_id,
                i.material_id,
                m.material_name,
                m.unit,
                i.closing_stock as available_quantity,
                i.weighted_avg_cost,
                m.category,
                m.short_code,
                p.traceable_code as latest_purchase_code
            FROM inventory i
            JOIN materials m ON i.material_id = m.material_id
            LEFT JOIN purchases p ON p.supplier_id = m.supplier_id
            LEFT JOIN purchase_items pi ON pi.purchase_id = p.purchase_id AND pi.material_id = m.material_id
            WHERE m.category = 'Seeds' 
                AND i.closing_stock > 0
            ORDER BY i.material_id, p.purchase_date DESC
        """)
        
        seeds = []
        total_value = 0
        for row in cur.fetchall():
            value = float(row[4]) * float(row[5])
            total_value += value
            seeds.append({
                'inventory_id': row[0],
                'material_id': row[1],
                'material_name': row[2],
                'unit': row[3],
                'available_quantity': float(row[4]),
                'weighted_avg_cost': float(row[5]),
                'category': row[6],
                'short_code': row[7],
                'latest_purchase_code': row[8],
                'total_value': value
            })
        
        return jsonify({
            'success': True,
            'seeds': seeds,
            'count': len(seeds),
            'total_inventory_value': total_value
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@batch_bp.route('/api/cost_elements_for_batch', methods=['GET'])
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
            ORDER BY 
                CASE category 
                    WHEN 'Labor' THEN 1 
                    WHEN 'Utilities' THEN 2 
                    WHEN 'Maintenance' THEN 3 
                END,
                element_name
        """)
        
        cost_elements = []
        categories = {}
        
        for row in cur.fetchall():
            element = {
                'element_id': row[0],
                'element_name': row[1],
                'category': row[2],
                'unit_type': row[3],
                'default_rate': float(row[4]),
                'calculation_method': row[5]
            }
            cost_elements.append(element)
            
            # Group by category
            if row[2] not in categories:
                categories[row[2]] = []
            categories[row[2]].append(element)
        
        return jsonify({
            'success': True,
            'cost_elements': cost_elements,
            'cost_elements_by_category': categories,
            'count': len(cost_elements)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@batch_bp.route('/api/oil_cake_rates', methods=['GET'])
def get_oil_cake_rates():
    """Get current oil cake and sludge rates for estimation"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Try to get rates from database if table exists
        try:
            cur.execute("""
                SELECT oil_type, cake_rate, sludge_rate 
                FROM oil_cake_rate_master 
                WHERE active = true
            """)
            
            rates = {}
            for row in cur.fetchall():
                rates[row[0]] = {
                    'cake_rate': float(row[1]),
                    'sludge_rate': float(row[2])
                }
                
            if rates:
                return jsonify({
                    'success': True,
                    'rates': rates,
                    'source': 'database'
                })
        except:
            # Table doesn't exist, use defaults
            pass
        
        # Default rates if no table or no data
        oil_cake_rates = {
            'Groundnut': {'cake_rate': 30.00, 'sludge_rate': 10.00},
            'Sesame': {'cake_rate': 35.00, 'sludge_rate': 12.00},
            'Coconut': {'cake_rate': 25.00, 'sludge_rate': 8.00},
            'Mustard': {'cake_rate': 28.00, 'sludge_rate': 9.00}
        }
        
        return jsonify({
            'success': True,
            'rates': oil_cake_rates,
            'source': 'default'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@batch_bp.route('/api/add_batch', methods=['POST'])
def add_batch():
    """Create a new batch production record with comprehensive validation and traceability"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Debug logging
        print(f"Received batch data: {data}")
        
        # Validate required fields
        required_fields = ['oil_type', 'batch_description', 'production_date', 
                          'material_id', 'seed_quantity_before_drying', 
                          'seed_quantity_after_drying', 'oil_yield', 
                          'cake_yield', 'cake_estimated_rate']
        
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] is None or data[field] == '':
                missing_fields.append(field)
        
        if missing_fields:
            return jsonify({
                'success': False,
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400
        
        # Parse date
        production_date = parse_date(data['production_date'])
        
        # Generate batch code
        date_str = data['production_date'].replace('-', '')
        batch_code = f"BATCH-{date_str}-{data['batch_description']}"
        
        # Get seed purchase traceable code for this material
        seed_purchase_code = data.get('seed_purchase_code')
        if not seed_purchase_code:
            # Try to get the latest purchase code for this material
            cur.execute("""
                SELECT p.traceable_code
                FROM purchases p
                JOIN purchase_items pi ON p.purchase_id = pi.purchase_id
                WHERE pi.material_id = %s 
                    AND p.traceable_code IS NOT NULL
                ORDER BY p.purchase_date DESC
                LIMIT 1
            """, (data['material_id'],))
            
            result = cur.fetchone()
            if result:
                seed_purchase_code = result[0]
            else:
                return jsonify({
                    'success': False,
                    'error': 'No purchase traceable code found for this seed. The seed must have been purchased with traceability enabled.'
                }), 400
        
        # Generate batch traceable code
        try:
            batch_traceable_code = generate_batch_traceable_code(
                data['material_id'],
                seed_purchase_code,
                production_date,
                cur
            )
        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Error generating batch traceable code: {str(e)}'
            }), 500
        
        # Safely convert values to Decimal with validation
        seed_qty_before = safe_decimal(data.get('seed_quantity_before_drying', 0))
        seed_qty_after = safe_decimal(data.get('seed_quantity_after_drying', 0))
        oil_yield = safe_decimal(data.get('oil_yield', 0))
        cake_yield = safe_decimal(data.get('cake_yield', 0))
        sludge_yield = safe_decimal(data.get('sludge_yield', 0))
        
        # Validate quantities
        validations = [
            validate_positive_number(seed_qty_before, 'Seed quantity before drying'),
            validate_positive_number(seed_qty_after, 'Seed quantity after drying'),
            validate_positive_number(oil_yield, 'Oil yield')
        ]
        
        for is_valid, error_msg in validations:
            if not is_valid:
                return jsonify({'success': False, 'error': error_msg}), 400
        
        if seed_qty_after > seed_qty_before:
            return jsonify({
                'success': False,
                'error': 'Seed quantity after drying cannot exceed quantity before drying'
            }), 400
        
        # Check seed availability
        cur.execute("""
            SELECT closing_stock FROM inventory 
            WHERE material_id = %s
            ORDER BY inventory_id DESC LIMIT 1
        """, (data['material_id'],))
        
        available_stock = cur.fetchone()
        if not available_stock or float(available_stock[0]) < float(seed_qty_before):
            return jsonify({
                'success': False,
                'error': f'Insufficient seed stock. Available: {available_stock[0] if available_stock else 0} kg'
            }), 400
        
        drying_loss = seed_qty_before - seed_qty_after
        
        # Calculate percentages
        oil_yield_percent = (oil_yield / seed_qty_after * 100) if seed_qty_after > 0 else 0
        cake_yield_percent = (cake_yield / seed_qty_after * 100) if seed_qty_after > 0 else 0
        sludge_yield_percent = (sludge_yield / seed_qty_after * 100) if seed_qty_after > 0 else 0
        
        # Begin transaction
        cur.execute("BEGIN")
        
        # Insert batch record with traceable code
        cur.execute("""
            INSERT INTO batch (
                batch_code, oil_type, seed_quantity_before_drying,
                seed_quantity_after_drying, drying_loss, oil_yield,
                oil_yield_percent, oil_cake_yield, oil_cake_yield_percent,
                sludge_yield, sludge_yield_percent, production_date, recipe_id,
                traceable_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            None,  # recipe_id - can be added later
            batch_traceable_code
        ))
        
        batch_id = cur.fetchone()[0]
        
        # Process cost details
        total_production_cost = safe_decimal(data.get('seed_cost_total', 0))
        
        # Insert all cost elements with validation
        cost_details = data.get('cost_details', [])
        for cost_item in cost_details:
            element_name = cost_item.get('element_name', '')
            master_rate = safe_float(cost_item.get('master_rate', 0))
            
            # Handle override rate
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
        
        # Calculate net oil cost
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
            SELECT inventory_id, closing_stock, weighted_avg_cost 
            FROM inventory 
            WHERE material_id IS NULL 
                AND product_id IS NULL
                AND oil_type = %s
                AND is_bulk_oil = true
                AND source_type = 'extraction'
            ORDER BY inventory_id DESC
            LIMIT 1
        """, (data['oil_type'],))
        
        oil_inv = cur.fetchone()
        
        if oil_inv:
            # Update existing oil inventory with weighted average
            old_stock = float(oil_inv[1])
            old_avg_cost = float(oil_inv[2])
            new_stock = old_stock + float(oil_yield)
            
            # Calculate new weighted average
            total_value = (old_stock * old_avg_cost) + (float(oil_yield) * float(oil_cost_per_kg))
            new_avg_cost = total_value / new_stock if new_stock > 0 else float(oil_cost_per_kg)
            
            cur.execute("""
                UPDATE inventory
                SET closing_stock = %s,
                    weighted_avg_cost = %s,
                    last_updated = %s
                WHERE inventory_id = %s
            """, (new_stock, new_avg_cost, production_date, oil_inv[0]))
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
            'traceable_code': batch_traceable_code,
            'oil_cost_per_kg': float(oil_cost_per_kg),
            'total_oil_produced': float(oil_yield),
            'net_oil_cost': float(net_oil_cost),
            'message': f'Batch {batch_code} created successfully with traceable code {batch_traceable_code}!'
        }), 201
        
    except Exception as e:
        conn.rollback()
        print(f"Error in add_batch: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@batch_bp.route('/api/batch_history', methods=['GET'])
def get_batch_history():
    """Get batch production history with filters, analytics, and traceable codes"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get query parameters
        limit = request.args.get('limit', 50, type=int)
        oil_type = request.args.get('oil_type', None)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Build query with filters
        query = """
            SELECT 
                b.batch_id,
                b.batch_code,
                b.oil_type,
                b.production_date,
                b.seed_quantity_before_drying,
                b.seed_quantity_after_drying,
                b.drying_loss,
                b.oil_yield,
                b.oil_yield_percent,
                b.oil_cake_yield,
                b.oil_cake_yield_percent,
                b.sludge_yield,
                b.sludge_yield_percent,
                b.total_production_cost,
                b.net_oil_cost,
                b.oil_cost_per_kg,
                b.cake_estimated_rate,
                b.sludge_estimated_rate,
                COALESCE(b.cake_sold_quantity, 0) as cake_sold,
                COALESCE(b.oil_cake_yield - b.cake_sold_quantity, b.oil_cake_yield) as cake_remaining,
                b.traceable_code
            FROM batch b
            WHERE 1=1
        """
        
        params = []
        
        if oil_type:
            query += " AND b.oil_type = %s"
            params.append(oil_type)
            
        if start_date:
            query += " AND b.production_date >= %s"
            params.append(parse_date(start_date))
            
        if end_date:
            query += " AND b.production_date <= %s"
            params.append(parse_date(end_date))
            
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
                'seed_quantity_before': float(row[4]),
                'seed_quantity_after': float(row[5]),
                'drying_loss': float(row[6]),
                'oil_yield': float(row[7]),
                'oil_yield_percent': float(row[8]),
                'cake_yield': float(row[9]),
                'cake_yield_percent': float(row[10]),
                'sludge_yield': float(row[11]) if row[11] else 0,
                'sludge_yield_percent': float(row[12]) if row[12] else 0,
                'total_production_cost': float(row[13]),
                'net_oil_cost': float(row[14]),
                'oil_cost_per_kg': float(row[15]),
                'cake_rate': float(row[16]) if row[16] else 0,
                'sludge_rate': float(row[17]) if row[17] else 0,
                'cake_sold': float(row[18]),
                'cake_remaining': float(row[19]),
                'traceable_code': row[20]
            })
        
        # Get summary statistics
        summary_query = """
            SELECT 
                COUNT(*) as total_batches,
                COALESCE(SUM(seed_quantity_before_drying), 0) as total_seeds_used,
                COALESCE(SUM(oil_yield), 0) as total_oil_produced,
                COALESCE(SUM(oil_cake_yield), 0) as total_cake_produced,
                COALESCE(SUM(sludge_yield), 0) as total_sludge_produced,
                COALESCE(AVG(oil_yield_percent), 0) as avg_oil_yield_percent,
                COALESCE(AVG(oil_cost_per_kg), 0) as avg_oil_cost,
                COALESCE(SUM(total_production_cost), 0) as total_production_cost,
                COALESCE(SUM(net_oil_cost), 0) as total_net_oil_cost
            FROM batch
            WHERE 1=1
        """
        
        # Apply same filters
        if oil_type:
            summary_query += " AND oil_type = %s"
        if start_date:
            summary_query += " AND production_date >= %s"
        if end_date:
            summary_query += " AND production_date <= %s"
            
        cur.execute(summary_query, params[:-1])  # Exclude limit
        stats = cur.fetchone()
        
        # Get oil type breakdown
        cur.execute("""
            SELECT 
                oil_type,
                COUNT(*) as batch_count,
                COALESCE(SUM(oil_yield), 0) as total_oil,
                COALESCE(AVG(oil_yield_percent), 0) as avg_yield_percent,
                COALESCE(AVG(oil_cost_per_kg), 0) as avg_cost
            FROM batch
            GROUP BY oil_type
            ORDER BY total_oil DESC
        """)
        
        oil_type_summary = []
        for row in cur.fetchall():
            oil_type_summary.append({
                'oil_type': row[0],
                'batch_count': row[1],
                'total_oil': float(row[2]),
                'avg_yield_percent': float(row[3]),
                'avg_cost': float(row[4])
            })
        
        return jsonify({
            'success': True,
            'batches': batches,
            'count': len(batches),
            'summary': {
                'total_batches': stats[0],
                'total_seeds_used': float(stats[1]),
                'total_oil_produced': float(stats[2]),
                'total_cake_produced': float(stats[3]),
                'total_sludge_produced': float(stats[4]),
                'avg_oil_yield_percent': float(stats[5]),
                'avg_oil_cost': float(stats[6]),
                'total_production_cost': float(stats[7]),
                'total_net_oil_cost': float(stats[8])
            },
            'oil_type_summary': oil_type_summary
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)
