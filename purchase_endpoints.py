from flask import Flask, request, jsonify
from flask_cors import CORS
from db_utils import get_db_connection, close_connection
from inventory_utils import update_inventory

app = Flask(__name__)
CORS(app)  # Allow frontend access

@app.route('/api/add_purchase', methods=['POST'])
def add_purchase():
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
        # Update materials table with new weighted avg (simplified)
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

if __name__ == '__main__':
    app.run()