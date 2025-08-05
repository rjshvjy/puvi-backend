"""
Material Sales Module for PUVI Oil Manufacturing System
Handles sales of by-products (oil cake, sludge, etc.) with FIFO allocation and retroactive cost adjustments
"""

from flask import Blueprint, request, jsonify
from decimal import Decimal
from db_utils import get_db_connection, close_connection
from utils.date_utils import parse_date, integer_to_date, get_current_day_number
from utils.validation import safe_decimal, safe_float, validate_required_fields

# Create Blueprint
material_sales_bp = Blueprint('material_sales', __name__)

# Define by-product types that can be sold
BYPRODUCT_TYPES = {
    'oil_cake': {
        'name': 'Oil Cake',
        'source': 'batch_production',
        'unit': 'kg',
        'affects_cost': 'oil_cost_per_kg'
    },
    'sludge': {
        'name': 'Sludge',
        'source': 'batch_production', 
        'unit': 'kg',
        'affects_cost': 'oil_cost_per_kg'
    },
    'gunny_bags': {
        'name': 'Scrap Gunny Bags',
        'source': 'material_storage',
        'unit': 'Nos',
        'affects_cost': 'material_cost'
    }
}

@material_sales_bp.route('/api/byproduct_types', methods=['GET'])
def get_byproduct_types():
    """Get available by-product types for sale"""
    try:
        types_list = []
        for key, value in BYPRODUCT_TYPES.items():
            types_list.append({
                'type_code': key,
                'type_name': value['name'],
                'source': value['source'],
                'unit': value['unit']
            })
        
        return jsonify({
            'success': True,
            'byproduct_types': types_list
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@material_sales_bp.route('/api/material_sales_inventory', methods=['GET'])
def get_material_sales_inventory():
    """Get available inventory for material sales (oil cake, sludge, etc.)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        byproduct_type = request.args.get('type', 'oil_cake')
        oil_type = request.args.get('oil_type')
        
        inventory_items = []
        
        if byproduct_type == 'oil_cake':
            # Get oil cake inventory from batches
            query = """
                SELECT 
                    oci.cake_inventory_id,
                    oci.batch_id,
                    b.batch_code,
                    oci.oil_type,
                    oci.quantity_produced,
                    oci.quantity_remaining,
                    oci.estimated_rate,
                    oci.production_date,
                    b.traceable_code,
                    CURRENT_DATE - (DATE '1970-01-01' + oci.production_date * INTERVAL '1 day') as age_days
                FROM oil_cake_inventory oci
                JOIN batch b ON oci.batch_id = b.batch_id
                WHERE oci.quantity_remaining > 0
            """
            
            params = []
            if oil_type:
                query += " AND oci.oil_type = %s"
                params.append(oil_type)
            
            query += " ORDER BY oci.production_date ASC"  # FIFO order
            
            cur.execute(query, params)
            
            for row in cur.fetchall():
                inventory_items.append({
                    'inventory_id': row[0],
                    'batch_id': row[1],
                    'batch_code': row[2],
                    'oil_type': row[3],
                    'quantity_produced': float(row[4]),
                    'quantity_remaining': float(row[5]),
                    'estimated_rate': float(row[6]),
                    'production_date': integer_to_date(row[7]),
                    'traceable_code': row[8],
                    'age_days': row[9].days if row[9] else 0,
                    'type': 'oil_cake',
                    'unit': 'kg'
                })
                
        elif byproduct_type == 'sludge':
            # Get sludge inventory from batches
            query = """
                SELECT 
                    b.batch_id,
                    b.batch_code,
                    b.oil_type,
                    b.sludge_yield as quantity_produced,
                    b.sludge_yield - COALESCE(b.sludge_sold_quantity, 0) as quantity_remaining,
                    b.sludge_estimated_rate as estimated_rate,
                    b.production_date,
                    b.traceable_code,
                    CURRENT_DATE - (DATE '1970-01-01' + b.production_date * INTERVAL '1 day') as age_days
                FROM batch b
                WHERE b.sludge_yield > 0 
                    AND (b.sludge_yield - COALESCE(b.sludge_sold_quantity, 0)) > 0
            """
            
            params = []
            if oil_type:
                query += " AND b.oil_type = %s"
                params.append(oil_type)
            
            query += " ORDER BY b.production_date ASC"  # FIFO order
            
            cur.execute(query, params)
            
            for row in cur.fetchall():
                inventory_items.append({
                    'inventory_id': row[0],  # Using batch_id as inventory_id for sludge
                    'batch_id': row[0],
                    'batch_code': row[1],
                    'oil_type': row[2],
                    'quantity_produced': float(row[3]) if row[3] else 0,
                    'quantity_remaining': float(row[4]) if row[4] else 0,
                    'estimated_rate': float(row[5]) if row[5] else 0,
                    'production_date': integer_to_date(row[6]),
                    'traceable_code': row[7],
                    'age_days': row[8].days if row[8] else 0,
                    'type': 'sludge',
                    'unit': 'kg'
                })
        
        # Get distinct oil types for filtering
        cur.execute("""
            SELECT DISTINCT oil_type 
            FROM (
                SELECT oil_type FROM oil_cake_inventory WHERE quantity_remaining > 0
                UNION
                SELECT oil_type FROM batch WHERE sludge_yield > 0
            ) AS combined
            ORDER BY oil_type
        """)
        
        oil_types = [row[0] for row in cur.fetchall()]
        
        # Calculate summary statistics
        total_quantity = sum(item['quantity_remaining'] for item in inventory_items)
        total_value = sum(item['quantity_remaining'] * item['estimated_rate'] for item in inventory_items)
        
        return jsonify({
            'success': True,
            'inventory_items': inventory_items,
            'oil_types': oil_types,
            'summary': {
                'total_quantity': total_quantity,
                'total_estimated_value': total_value,
                'item_count': len(inventory_items),
                'oldest_stock_days': max([item['age_days'] for item in inventory_items], default=0)
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@material_sales_bp.route('/api/add_material_sale', methods=['POST'])
def add_material_sale():
    """Record a material sale with FIFO allocation and cost adjustment"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['sale_date', 'buyer_name', 'quantity_sold', 'sale_rate', 'byproduct_type']
        is_valid, missing_fields = validate_required_fields(data, required_fields)
        
        if not is_valid:
            return jsonify({
                'success': False,
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400
        
        byproduct_type = data['byproduct_type']
        quantity_to_sell = safe_decimal(data['quantity_sold'])
        sale_rate = safe_decimal(data['sale_rate'])
        sale_date = parse_date(data['sale_date'])
        
        if quantity_to_sell <= 0:
            return jsonify({'success': False, 'error': 'Quantity must be greater than 0'}), 400
        
        # Begin transaction
        cur.execute("BEGIN")
        
        # Get available inventory based on type (FIFO order)
        available_batches = []
        
        if byproduct_type == 'oil_cake':
            cur.execute("""
                SELECT 
                    oci.cake_inventory_id,
                    oci.batch_id,
                    oci.quantity_remaining,
                    oci.estimated_rate,
                    b.oil_type
                FROM oil_cake_inventory oci
                JOIN batch b ON oci.batch_id = b.batch_id
                WHERE oci.quantity_remaining > 0
                    AND (%s IS NULL OR b.oil_type = %s)
                ORDER BY oci.production_date ASC
            """, (data.get('oil_type'), data.get('oil_type')))
            
            for row in cur.fetchall():
                available_batches.append({
                    'inventory_id': row[0],
                    'batch_id': row[1],
                    'quantity_remaining': Decimal(str(row[2])),
                    'estimated_rate': Decimal(str(row[3])),
                    'oil_type': row[4]
                })
                
        elif byproduct_type == 'sludge':
            cur.execute("""
                SELECT 
                    b.batch_id,
                    b.sludge_yield - COALESCE(b.sludge_sold_quantity, 0) as quantity_remaining,
                    b.sludge_estimated_rate,
                    b.oil_type
                FROM batch b
                WHERE b.sludge_yield > 0 
                    AND (b.sludge_yield - COALESCE(b.sludge_sold_quantity, 0)) > 0
                    AND (%s IS NULL OR b.oil_type = %s)
                ORDER BY b.production_date ASC
            """, (data.get('oil_type'), data.get('oil_type')))
            
            for row in cur.fetchall():
                available_batches.append({
                    'inventory_id': row[0],  # batch_id for sludge
                    'batch_id': row[0],
                    'quantity_remaining': Decimal(str(row[1])),
                    'estimated_rate': Decimal(str(row[2])) if row[2] else Decimal('0'),
                    'oil_type': row[3]
                })
        
        # Check if enough inventory available
        total_available = sum(batch['quantity_remaining'] for batch in available_batches)
        if total_available < quantity_to_sell:
            conn.rollback()
            return jsonify({
                'success': False,
                'error': f'Insufficient inventory. Available: {float(total_available)} kg'
            }), 400
        
        # Insert sale record
        cur.execute("""
            INSERT INTO oil_cake_sales (
                sale_date, invoice_number, buyer_name, oil_type,
                grade, quantity_sold, sale_rate, total_amount,
                transport_cost, net_rate, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING sale_id
        """, (
            sale_date,
            data.get('invoice_number', f"INV-{sale_date}-{data['buyer_name'][:3].upper()}"),
            data['buyer_name'],
            available_batches[0]['oil_type'] if available_batches else data.get('oil_type'),
            byproduct_type,  # Using grade field to store type
            float(quantity_to_sell),
            float(sale_rate),
            float(quantity_to_sell * sale_rate),
            float(safe_decimal(data.get('transport_cost', 0))),
            float(sale_rate - safe_decimal(data.get('transport_cost', 0)) / quantity_to_sell),
            data.get('notes', '')
        ))
        
        sale_id = cur.fetchone()[0]
        
        # FIFO allocation
        remaining_quantity = quantity_to_sell
        allocations = []
        total_adjustment = Decimal('0')
        
        for batch in available_batches:
            if remaining_quantity <= 0:
                break
            
            # Calculate allocation for this batch
            allocation_qty = min(remaining_quantity, batch['quantity_remaining'])
            
            # Calculate cost adjustment
            estimated_revenue = allocation_qty * batch['estimated_rate']
            actual_revenue = allocation_qty * sale_rate
            cost_adjustment = estimated_revenue - actual_revenue  # Negative if sold for less
            
            # Insert allocation record
            cur.execute("""
                INSERT INTO oil_cake_sale_allocations (
                    sale_id, batch_id, quantity_allocated,
                    original_estimate_rate, actual_sale_rate,
                    cost_adjustment_per_kg, oil_cost_adjustment
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                sale_id,
                batch['batch_id'],
                float(allocation_qty),
                float(batch['estimated_rate']),
                float(sale_rate),
                float(batch['estimated_rate'] - sale_rate),
                float(cost_adjustment)
            ))
            
            # Update inventory based on type
            if byproduct_type == 'oil_cake':
                cur.execute("""
                    UPDATE oil_cake_inventory
                    SET quantity_remaining = quantity_remaining - %s
                    WHERE cake_inventory_id = %s
                """, (float(allocation_qty), batch['inventory_id']))
                
                # Update batch cake sold quantity
                cur.execute("""
                    UPDATE batch
                    SET cake_sold_quantity = COALESCE(cake_sold_quantity, 0) + %s,
                        cake_actual_rate = %s
                    WHERE batch_id = %s
                """, (float(allocation_qty), float(sale_rate), batch['batch_id']))
                
            elif byproduct_type == 'sludge':
                cur.execute("""
                    UPDATE batch
                    SET sludge_sold_quantity = COALESCE(sludge_sold_quantity, 0) + %s,
                        sludge_actual_rate = %s
                    WHERE batch_id = %s
                """, (float(allocation_qty), float(sale_rate), batch['batch_id']))
            
            # Retroactively adjust batch oil cost
            if byproduct_type in ['oil_cake', 'sludge']:
                # Get current batch details
                cur.execute("""
                    SELECT oil_yield, total_production_cost, 
                           oil_cake_yield, cake_estimated_rate,
                           sludge_yield, sludge_estimated_rate
                    FROM batch
                    WHERE batch_id = %s
                """, (batch['batch_id'],))
                
                batch_details = cur.fetchone()
                if batch_details:
                    oil_yield = Decimal(str(batch_details[0]))
                    total_cost = Decimal(str(batch_details[1]))
                    cake_yield = Decimal(str(batch_details[2])) if batch_details[2] else Decimal('0')
                    cake_est_rate = Decimal(str(batch_details[3])) if batch_details[3] else Decimal('0')
                    sludge_yield = Decimal(str(batch_details[4])) if batch_details[4] else Decimal('0')
                    sludge_est_rate = Decimal(str(batch_details[5])) if batch_details[5] else Decimal('0')
                    
                    # Recalculate net oil cost with adjustment
                    if byproduct_type == 'oil_cake':
                        # Replace estimated cake revenue with actual for sold portion
                        estimated_cake_revenue = cake_yield * cake_est_rate
                        actual_cake_revenue = allocation_qty * sale_rate + (cake_yield - allocation_qty) * cake_est_rate
                        adjustment = estimated_cake_revenue - actual_cake_revenue
                    else:  # sludge
                        estimated_sludge_revenue = sludge_yield * sludge_est_rate
                        actual_sludge_revenue = allocation_qty * sale_rate + (sludge_yield - allocation_qty) * sludge_est_rate
                        adjustment = estimated_sludge_revenue - actual_sludge_revenue
                    
                    # Update net oil cost
                    new_net_oil_cost = total_cost - (cake_yield * cake_est_rate) - (sludge_yield * sludge_est_rate) + adjustment
                    new_oil_cost_per_kg = new_net_oil_cost / oil_yield if oil_yield > 0 else Decimal('0')
                    
                    cur.execute("""
                        UPDATE batch
                        SET net_oil_cost = %s,
                            oil_cost_per_kg = %s
                        WHERE batch_id = %s
                    """, (float(new_net_oil_cost), float(new_oil_cost_per_kg), batch['batch_id']))
            
            allocations.append({
                'batch_id': batch['batch_id'],
                'quantity': float(allocation_qty),
                'adjustment': float(cost_adjustment)
            })
            
            total_adjustment += cost_adjustment
            remaining_quantity -= allocation_qty
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'sale_id': sale_id,
            'quantity_sold': float(quantity_to_sell),
            'sale_rate': float(sale_rate),
            'total_amount': float(quantity_to_sell * sale_rate),
            'allocations': allocations,
            'total_cost_adjustment': float(total_adjustment),
            'message': f'Sale recorded successfully with {len(allocations)} batch allocations'
        }), 201
        
    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@material_sales_bp.route('/api/material_sales_history', methods=['GET'])
def get_material_sales_history():
    """Get material sales history with allocations and adjustments"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        limit = request.args.get('limit', 50, type=int)
        byproduct_type = request.args.get('type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Build query
        query = """
            SELECT 
                s.sale_id,
                s.sale_date,
                s.invoice_number,
                s.buyer_name,
                s.oil_type,
                s.grade as byproduct_type,
                s.quantity_sold,
                s.sale_rate,
                s.total_amount,
                s.transport_cost,
                s.net_rate,
                s.notes,
                COUNT(a.allocation_id) as batch_count,
                COALESCE(SUM(a.oil_cost_adjustment), 0) as total_adjustment
            FROM oil_cake_sales s
            LEFT JOIN oil_cake_sale_allocations a ON s.sale_id = a.sale_id
            WHERE 1=1
        """
        
        params = []
        
        if byproduct_type:
            query += " AND s.grade = %s"
            params.append(byproduct_type)
        
        if start_date:
            query += " AND s.sale_date >= %s"
            params.append(parse_date(start_date))
        
        if end_date:
            query += " AND s.sale_date <= %s"
            params.append(parse_date(end_date))
        
        query += """
            GROUP BY s.sale_id, s.sale_date, s.invoice_number, s.buyer_name,
                     s.oil_type, s.grade, s.quantity_sold, s.sale_rate,
                     s.total_amount, s.transport_cost, s.net_rate, s.notes
            ORDER BY s.sale_date DESC, s.sale_id DESC
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        
        sales = []
        for row in cur.fetchall():
            sale = {
                'sale_id': row[0],
                'sale_date': integer_to_date(row[1]),
                'invoice_number': row[2],
                'buyer_name': row[3],
                'oil_type': row[4],
                'byproduct_type': row[5] or 'oil_cake',
                'quantity_sold': float(row[6]),
                'sale_rate': float(row[7]),
                'total_amount': float(row[8]),
                'transport_cost': float(row[9]) if row[9] else 0,
                'net_rate': float(row[10]) if row[10] else float(row[7]),
                'notes': row[11],
                'batch_count': row[12],
                'total_adjustment': float(row[13])
            }
            
            # Get allocations for this sale
            cur.execute("""
                SELECT 
                    a.batch_id,
                    b.batch_code,
                    a.quantity_allocated,
                    a.original_estimate_rate,
                    a.actual_sale_rate,
                    a.oil_cost_adjustment
                FROM oil_cake_sale_allocations a
                JOIN batch b ON a.batch_id = b.batch_id
                WHERE a.sale_id = %s
                ORDER BY a.allocation_id
            """, (row[0],))
            
            allocations = []
            for alloc_row in cur.fetchall():
                allocations.append({
                    'batch_id': alloc_row[0],
                    'batch_code': alloc_row[1],
                    'quantity_allocated': float(alloc_row[2]),
                    'original_estimate_rate': float(alloc_row[3]),
                    'actual_sale_rate': float(alloc_row[4]),
                    'adjustment': float(alloc_row[5])
                })
            
            sale['allocations'] = allocations
            sales.append(sale)
        
        # Get summary statistics
        cur.execute("""
            SELECT 
                COUNT(DISTINCT s.sale_id) as total_sales,
                COALESCE(SUM(s.quantity_sold), 0) as total_quantity,
                COALESCE(SUM(s.total_amount), 0) as total_revenue,
                COALESCE(SUM(a.oil_cost_adjustment), 0) as total_adjustments,
                COUNT(DISTINCT s.buyer_name) as unique_buyers
            FROM oil_cake_sales s
            LEFT JOIN oil_cake_sale_allocations a ON s.sale_id = a.sale_id
            WHERE 1=1
        """ + (" AND s.grade = %s" if byproduct_type else ""), 
        [byproduct_type] if byproduct_type else [])
        
        summary = cur.fetchone()
        
        return jsonify({
            'success': True,
            'sales': sales,
            'count': len(sales),
            'summary': {
                'total_sales': summary[0],
                'total_quantity_sold': float(summary[1]),
                'total_revenue': float(summary[2]),
                'total_cost_adjustments': float(summary[3]),
                'unique_buyers': summary[4]
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@material_sales_bp.route('/api/cost_reconciliation_report', methods=['GET'])
def get_cost_reconciliation_report():
    """Get detailed cost reconciliation report showing impact of by-product sales"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get batches with reconciliation details
        cur.execute("""
            SELECT 
                b.batch_id,
                b.batch_code,
                b.oil_type,
                b.production_date,
                b.oil_yield,
                b.oil_cost_per_kg as current_cost,
                b.oil_cake_yield,
                b.cake_estimated_rate,
                b.cake_actual_rate,
                COALESCE(b.cake_sold_quantity, 0) as cake_sold,
                b.sludge_yield,
                b.sludge_estimated_rate,
                b.sludge_actual_rate,
                COALESCE(b.sludge_sold_quantity, 0) as sludge_sold,
                b.total_production_cost,
                b.net_oil_cost
            FROM batch b
            WHERE (b.cake_sold_quantity > 0 OR b.sludge_sold_quantity > 0)
            ORDER BY b.production_date DESC
        """)
        
        reconciliation_data = []
        for row in cur.fetchall():
            # Calculate adjustments
            cake_adjustment = 0
            if row[9] > 0:  # cake sold
                cake_adjustment = float(row[9]) * (float(row[7] or 0) - float(row[8] or 0))
            
            sludge_adjustment = 0
            if row[13] > 0:  # sludge sold
                sludge_adjustment = float(row[13]) * (float(row[11] or 0) - float(row[12] or 0))
            
            total_adjustment = cake_adjustment + sludge_adjustment
            
            reconciliation_data.append({
                'batch_id': row[0],
                'batch_code': row[1],
                'oil_type': row[2],
                'production_date': integer_to_date(row[3]),
                'oil_yield': float(row[4]),
                'current_oil_cost': float(row[5]),
                'cake_details': {
                    'yield': float(row[6]) if row[6] else 0,
                    'estimated_rate': float(row[7]) if row[7] else 0,
                    'actual_rate': float(row[8]) if row[8] else 0,
                    'sold_quantity': float(row[9]),
                    'adjustment': cake_adjustment
                },
                'sludge_details': {
                    'yield': float(row[10]) if row[10] else 0,
                    'estimated_rate': float(row[11]) if row[11] else 0,
                    'actual_rate': float(row[12]) if row[12] else 0,
                    'sold_quantity': float(row[13]),
                    'adjustment': sludge_adjustment
                },
                'total_adjustment': total_adjustment,
                'total_production_cost': float(row[14]),
                'net_oil_cost': float(row[15])
            })
        
        return jsonify({
            'success': True,
            'reconciliation_data': reconciliation_data,
            'count': len(reconciliation_data)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)
