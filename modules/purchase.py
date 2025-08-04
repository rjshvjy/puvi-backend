"""
Purchase Module for PUVI Oil Manufacturing System
Handles all purchase-related operations including material purchases and inventory updates
"""

from flask import Blueprint, request, jsonify
from decimal import Decimal
from db_utils import get_db_connection, close_connection
from inventory_utils import update_inventory
from utils.date_utils import date_to_day_number
from utils.validation import safe_decimal

# Create Blueprint
purchase_bp = Blueprint('purchase', __name__)

@purchase_bp.route('/api/materials', methods=['GET'])
def get_materials():
    """Get all materials for dropdown selection"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT material_id, material_name, current_cost, gst_rate, unit, category
            FROM materials 
            ORDER BY category, material_name
        """)
        
        materials = []
        for row in cur.fetchall():
            materials.append({
                'material_id': row[0],
                'material_name': row[1],
                'current_cost': float(row[2]),
                'gst_rate': float(row[3]),
                'unit': row[4],
                'category': row[5] if len(row) > 5 else None
            })
        
        return jsonify({
            'success': True,
            'materials': materials,
            'count': len(materials)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    finally:
        close_connection(conn, cur)


@purchase_bp.route('/api/add_purchase', methods=['POST'])
def add_purchase():
    """Add a new purchase transaction"""
    data = request.json
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Validate required fields
        required_fields = ['material_id', 'quantity', 'cost_per_unit', 
                          'gst_rate', 'invoice_ref', 'purchase_date']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        
        if missing_fields:
            return jsonify({
                'success': False,
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400
        
        # Convert string values to Decimal for accurate calculation
        quantity = safe_decimal(data['quantity'])
        cost_per_unit = safe_decimal(data['cost_per_unit'])
        gst_rate = safe_decimal(data['gst_rate'])
        transport_cost = safe_decimal(data.get('transport_cost', 0))
        loading_charges = safe_decimal(data.get('loading_charges', 0))
        
        # Validate positive values
        if quantity <= 0:
            return jsonify({'success': False, 'error': 'Quantity must be greater than 0'}), 400
        if cost_per_unit <= 0:
            return jsonify({'success': False, 'error': 'Cost per unit must be greater than 0'}), 400
        
        # Calculate costs
        material_cost = quantity * cost_per_unit
        subtotal = material_cost + transport_cost + loading_charges
        gst_amount = subtotal * (gst_rate / 100)
        total_cost = subtotal + gst_amount
        landed_cost_per_unit = total_cost / quantity if quantity > 0 else 0
        
        # Convert date to day number
        purchase_date_day = date_to_day_number(data['purchase_date'])
        
        # Begin transaction
        cur.execute("BEGIN")
        
        # Insert purchase record
        cur.execute("""
            INSERT INTO purchases (
                material_id, quantity, cost_per_unit, gst_rate, 
                invoice_ref, purchase_date, supplier_name, batch_number,
                transport_cost, loading_charges, total_cost, landed_cost_per_unit
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING purchase_id
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
        
        purchase_id = cur.fetchone()[0]
        
        # Update inventory with landed cost
        update_inventory(
            data['material_id'], 
            float(quantity), 
            float(landed_cost_per_unit),
            conn, 
            cur
        )
        
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
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': 'Purchase added successfully',
            'purchase_id': purchase_id,
            'new_weighted_avg': float(new_avg),
            'landed_cost_per_unit': float(landed_cost_per_unit),
            'total_cost': float(total_cost)
        })
        
    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@purchase_bp.route('/api/purchase_history', methods=['GET'])
def get_purchase_history():
    """Get purchase history with optional filters"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        material_id = request.args.get('material_id', type=int)
        
        # Build query
        query = """
            SELECT 
                p.purchase_id,
                p.material_id,
                m.material_name,
                m.unit,
                p.quantity,
                p.cost_per_unit,
                p.gst_rate,
                p.transport_cost,
                p.loading_charges,
                p.total_cost,
                p.landed_cost_per_unit,
                p.invoice_ref,
                p.purchase_date,
                p.supplier_name,
                p.batch_number
            FROM purchases p
            JOIN materials m ON p.material_id = m.material_id
        """
        
        params = []
        if material_id:
            query += " WHERE p.material_id = %s"
            params.append(material_id)
            
        query += " ORDER BY p.purchase_date DESC, p.purchase_id DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, params)
        
        purchases = []
        for row in cur.fetchall():
            from utils.date_utils import integer_to_date
            purchases.append({
                'purchase_id': row[0],
                'material_id': row[1],
                'material_name': row[2],
                'unit': row[3],
                'quantity': float(row[4]),
                'cost_per_unit': float(row[5]),
                'gst_rate': float(row[6]),
                'transport_cost': float(row[7]) if row[7] else 0,
                'loading_charges': float(row[8]) if row[8] else 0,
                'total_cost': float(row[9]),
                'landed_cost_per_unit': float(row[10]),
                'invoice_ref': row[11],
                'purchase_date': integer_to_date(row[12]),
                'supplier_name': row[13],
                'batch_number': row[14]
            })
        
        # Get summary statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_purchases,
                COALESCE(SUM(total_cost), 0) as total_amount,
                COUNT(DISTINCT material_id) as unique_materials,
                COUNT(DISTINCT supplier_name) as unique_suppliers
            FROM purchases
        """)
        
        stats = cur.fetchone()
        
        return jsonify({
            'success': True,
            'purchases': purchases,
            'count': len(purchases),
            'summary': {
                'total_purchases': stats[0],
                'total_amount': float(stats[1]),
                'unique_materials': stats[2],
                'unique_suppliers': stats[3]
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@purchase_bp.route('/api/suppliers', methods=['GET'])
def get_suppliers():
    """Get list of suppliers"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                supplier_id,
                supplier_name,
                contact_person,
                phone,
                email,
                gst_number
            FROM suppliers
            ORDER BY supplier_name
        """)
        
        suppliers = []
        for row in cur.fetchall():
            suppliers.append({
                'supplier_id': row[0],
                'supplier_name': row[1],
                'contact_person': row[2],
                'phone': row[3],
                'email': row[4],
                'gst_number': row[5]
            })
        
        return jsonify({
            'success': True,
            'suppliers': suppliers,
            'count': len(suppliers)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)
