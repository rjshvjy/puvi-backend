from decimal import Decimal

def update_inventory(material_id, qty_purchased, cost_per_unit, conn, cur):
    # Convert to Decimal for proper calculation
    qty_purchased = Decimal(str(qty_purchased))
    cost_per_unit = Decimal(str(cost_per_unit))
    
    cur.execute("SELECT inventory_id, closing_stock, weighted_avg_cost FROM inventory WHERE material_id = %s", (material_id,))
    row = cur.fetchone()
    if row:
        inventory_id, current_stock, current_avg = row
        current_stock = Decimal(str(current_stock))
        current_avg = Decimal(str(current_avg))
        
        new_stock = current_stock + qty_purchased
        new_avg = (current_avg * current_stock + cost_per_unit * qty_purchased) / new_stock
        
        cur.execute("""
            UPDATE inventory 
            SET closing_stock = %s, weighted_avg_cost = %s, purchases = purchases + %s, last_updated = %s
            WHERE inventory_id = %s
        """, (float(new_stock), float(new_avg), float(qty_purchased), 45868, inventory_id))
    else:
        cur.execute("""
            INSERT INTO inventory (material_id, opening_stock, purchases, closing_stock, weighted_avg_cost, last_updated)
            VALUES (%s, 0, %s, %s, %s, %s)
        """, (material_id, float(qty_purchased), float(qty_purchased), float(cost_per_unit), 45868))
