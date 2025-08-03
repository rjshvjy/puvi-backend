from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from db_utils import get_db_connection, close_connection
from inventory_utils import update_inventory

app = Flask(__name__)
CORS(app)  # Allow frontend access

@app.route('/api/materials', methods=['GET'])
def get_materials():
    """Fetch all materials for the frontend dropdown."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT material_id, material_name, current_cost FROM materials")
        materials = cur.fetchall()
        return jsonify([{'material_id': m[0], 'material_name': m[1], 'current_cost': m[2]} for m in materials])
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        close_connection(conn, cur)

@app.route('/api/add_purchase', methods=['POST'])
def add_purchase():
    """Handle purchase data submission and update inventory/weighted average."""
    data = request.json  # e.g., {material_id: 1, quantity: 5000, cost_per_unit: 95.51, gst_rate: 5, invoice_ref: "INV-38"}
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO purchases (material_id, quantity, cost_per_unit, gst_rate, invoice_ref, purchase_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (data['material_id'], data['quantity'], data['cost_per_unit'], data['gst_rate'], data['invoice_ref'], 45868))
        update_inventory(data['material_id'], data['quantity'], data['cost_per_unit'], conn, cur)
        conn.commit()
        # Update materials table with new weighted average cost
        cur.execute("SELECT weighted_avg_cost FROM inventory WHERE material_id = %s", (data['material_id'],))
        new_avg = cur.fetchone()[0]
        cur.execute("UPDATE materials SET current_cost = %s WHERE material_id = %s", (new_avg, data['material_id']))
        conn.commit()
        return jsonify({'message': 'Purchase added', 'new_weighted_avg': new_avg})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        close_connection(conn, cur)

@app.route('/', methods=['GET'])
def home():
    """Return backend status with timestamp."""
    return jsonify({'status': 'Backend is running!', 'timestamp': datetime.now().isoformat()})

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check backend and database health."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM materials")
        count = cur.fetchone()[0]
        close_connection(conn, cur)
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'materials_count': count
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database': 'disconnected',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)  # For local testing only
