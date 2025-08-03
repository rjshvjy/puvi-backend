from datetime import datetime
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
