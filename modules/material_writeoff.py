"""
Material Writeoff Module for PUVI Oil Manufacturing System
Handles material writeoffs due to damage, expiry, returns, or other reasons
"""

from flask import Blueprint, request, jsonify
from db_utils import get_db_connection, close_connection
from utils.date_utils import parse_date, integer_to_date
from utils.validation import safe_float, validate_required_fields

# Create Blueprint
writeoff_bp = Blueprint('writeoff', __name__)

@writeoff_bp.route('/api/writeoff_reasons', methods=['GET'])
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
        
        # Group by category for better organization
        reasons_by_category = {}
        for reason in reasons:
            category = reason['category'] or 'Other'
            if category not in reasons_by_category:
                reasons_by_category[category] = []
            reasons_by_category[category].append(reason)
        
        return jsonify({
            'success': True,
            'reasons': reasons,
            'reasons_by_category': reasons_by_category,
            'count': len(reasons)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@writeoff_bp.route('/api/inventory_for_writeoff', methods=['GET'])
def get_inventory_for_writeoff():
    """Get materials with current inventory for writeoff selection"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get optional filter parameters
        category = request.args.get('category')
        
        query = """
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
        """
        
        params = []
        if category:
            query += " AND m.category = %s"
            params.append(category)
            
        query += " ORDER BY m.category, m.material_name"
        
        cur.execute(query, params)
        
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
                'last_updated': integer_to_date(row[7]) if row[7] else '',
                'total_value': float(row[5]) * float(row[6])
            })
        
        # Get category summary
        cur.execute("""
            SELECT 
                m.category,
                COUNT(DISTINCT m.material_id) as material_count,
                COALESCE(SUM(i.closing_stock * i.weighted_avg_cost), 0) as total_value
            FROM inventory i
            JOIN materials m ON i.material_id = m.material_id
            WHERE i.closing_stock > 0
            GROUP BY m.category
            ORDER BY m.category
        """)
        
        category_summary = []
        for row in cur.fetchall():
            category_summary.append({
                'category': row[0] or 'Uncategorized',
                'material_count': row[1],
                'total_value': float(row[2])
            })
        
        return jsonify({
            'success': True,
            'inventory_items': inventory_items,
            'count': len(inventory_items),
            'category_summary': category_summary
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@writeoff_bp.route('/api/add_writeoff', methods=['POST'])
def add_writeoff():
    """Record a material writeoff"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Validate required fields
        is_valid, missing_fields = validate_required_fields(
            data, 
            ['material_id', 'quantity', 'writeoff_date', 'reason_code']
        )
        
        if not is_valid:
            return jsonify({
                'success': False,
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400
        
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
            return jsonify({
                'success': False,
                'error': 'Material not found in inventory'
            }), 404
        
        current_stock = float(inv_row[0])
        weighted_avg_cost = float(inv_row[1])
        material_name = inv_row[2]
        unit = inv_row[3]
        
        # Validate quantity
        writeoff_qty = safe_float(data['quantity'])
        if writeoff_qty <= 0:
            return jsonify({
                'success': False,
                'error': 'Writeoff quantity must be greater than 0'
            }), 400
            
        if writeoff_qty > current_stock:
            return jsonify({
                'success': False,
                'error': f'Insufficient stock. Available: {current_stock} {unit}'
            }), 400
        
        # Calculate costs
        total_cost = writeoff_qty * weighted_avg_cost
        scrap_value = safe_float(data.get('scrap_value', 0))
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
        
        # Update inventory
        new_closing_stock = current_stock - writeoff_qty
        
        cur.execute("""
            UPDATE inventory
            SET closing_stock = %s,
                consumption = consumption + %s,
                last_updated = %s
            WHERE material_id = %s
                AND inventory_id = (
                    SELECT inventory_id FROM inventory 
                    WHERE material_id = %s 
                    ORDER BY inventory_id DESC LIMIT 1
                )
        """, (
            new_closing_stock,
            writeoff_qty,
            writeoff_date_int,
            data['material_id'],
            data['material_id']
        ))
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'writeoff_id': writeoff_id,
            'material_name': material_name,
            'quantity_written_off': writeoff_qty,
            'unit': unit,
            'total_cost': total_cost,
            'scrap_value': scrap_value,
            'net_loss': net_loss,
            'new_stock_balance': new_closing_stock,
            'message': f'Writeoff recorded successfully. {writeoff_qty} {unit} written off.'
        }), 201
        
    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@writeoff_bp.route('/api/writeoff_history', methods=['GET'])
def get_writeoff_history():
    """Get writeoff history with filters and summary"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        material_id = request.args.get('material_id', type=int)
        reason_code = request.args.get('reason_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Build query with filters
        query = """
            SELECT 
                w.*,
                m.material_name,
                m.unit,
                m.category
            FROM material_writeoffs w
            JOIN materials m ON w.material_id = m.material_id
            WHERE 1=1
        """
        
        params = []
        
        if material_id:
            query += " AND w.material_id = %s"
            params.append(material_id)
            
        if reason_code:
            query += " AND w.reason_code = %s"
            params.append(reason_code)
            
        if start_date:
            query += " AND w.writeoff_date >= %s"
            params.append(parse_date(start_date))
            
        if end_date:
            query += " AND w.writeoff_date <= %s"
            params.append(parse_date(end_date))
            
        query += " ORDER BY w.writeoff_date DESC, w.writeoff_id DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        
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
        
        # Get summary statistics with same filters
        summary_query = """
            SELECT 
                COUNT(*) as total_writeoffs,
                COALESCE(SUM(w.quantity), 0) as total_quantity,
                COALESCE(SUM(w.total_cost), 0) as total_cost_sum,
                COALESCE(SUM(w.scrap_value), 0) as total_scrap_value,
                COALESCE(SUM(w.net_loss), 0) as total_net_loss,
                COUNT(DISTINCT w.material_id) as unique_materials,
                COUNT(DISTINCT w.reason_code) as unique_reasons
            FROM material_writeoffs w
            WHERE 1=1
        """
        
        # Apply same filters to summary
        summary_params = params[:-1]  # Exclude limit
        if material_id:
            summary_query = summary_query.replace("WHERE 1=1", "WHERE 1=1 AND w.material_id = %s", 1)
        if reason_code:
            summary_query = summary_query.replace("WHERE 1=1", f"WHERE 1=1{' AND w.material_id = %s' if material_id else ''} AND w.reason_code = %s", 1)
        
        cur.execute(summary_query, summary_params)
        stats = cur.fetchone()
        
        # Get writeoff by reason summary
        cur.execute("""
            SELECT 
                w.reason_code,
                wr.reason_description,
                COUNT(*) as count,
                COALESCE(SUM(w.net_loss), 0) as total_loss
            FROM material_writeoffs w
            LEFT JOIN writeoff_reasons wr ON w.reason_code = wr.reason_code
            GROUP BY w.reason_code, wr.reason_description
            ORDER BY total_loss DESC
        """)
        
        reason_summary = []
        for row in cur.fetchall():
            reason_summary.append({
                'reason_code': row[0],
                'reason_description': row[1] or row[0],
                'count': row[2],
                'total_loss': float(row[3])
            })
        
        return jsonify({
            'success': True,
            'writeoffs': writeoffs,
            'count': len(writeoffs),
            'summary': {
                'total_writeoffs': stats[0],
                'total_quantity': float(stats[1]),
                'total_cost': float(stats[2]),
                'total_scrap_recovered': float(stats[3]),
                'total_net_loss': float(stats[4]),
                'unique_materials': stats[5],
                'unique_reasons': stats[6]
            },
            'reason_summary': reason_summary
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)
