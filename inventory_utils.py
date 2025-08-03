from decimal import Decimal
from datetime import datetime

def update_inventory(material_id, qty_purchased, landed_cost_per_unit, conn, cur):
    """
    Update inventory with new purchase, calculating weighted average based on landed cost
    
    Args:
        material_id: ID of the material
        qty_purchased: Quantity purchased
        landed_cost_per_unit: Total cost per unit including all charges and GST
        conn: Database connection
        cur: Database cursor
    """
    # Convert to Decimal for proper calculation
    qty_purchased = Decimal(str(qty_purchased))
    landed_cost_per_unit = Decimal(str(landed_cost_per_unit))
    
    # Get current day number
    current_day = (datetime.now() - datetime(1970, 1, 1)).days
    
    # Check if inventory record exists
    cur.execute(
        "SELECT inventory_id, closing_stock, weighted_avg_cost FROM inventory WHERE material_id = %s", 
        (material_id,)
    )
    row = cur.fetchone()
    
    if row:
        # Update existing inventory record
        inventory_id, current_stock, current_avg = row
        current_stock = Decimal(str(current_stock))
        current_avg = Decimal(str(current_avg))
        
        # Calculate new weighted average
        # Formula: (old_stock * old_avg + new_qty * new_cost) / (old_stock + new_qty)
        total_value = (current_stock * current_avg) + (qty_purchased * landed_cost_per_unit)
        new_stock = current_stock + qty_purchased
        new_avg = total_value / new_stock if new_stock > 0 else landed_cost_per_unit
        
        # Update inventory
        cur.execute("""
            UPDATE inventory
            SET closing_stock = %s, 
                weighted_avg_cost = %s, 
                purchases = purchases + %s, 
                last_updated = %s
            WHERE inventory_id = %s
        """, (
            float(new_stock), 
            float(new_avg), 
            float(qty_purchased), 
            current_day, 
            inventory_id
        ))
    else:
        # Create new inventory record
        cur.execute("""
            INSERT INTO inventory (
                material_id, 
                opening_stock, 
                purchases, 
                closing_stock, 
                weighted_avg_cost, 
                last_updated
            )
            VALUES (%s, 0, %s, %s, %s, %s)
        """, (
            material_id, 
            float(qty_purchased), 
            float(qty_purchased), 
            float(landed_cost_per_unit), 
            current_day
        ))
    
    # Log the transaction for audit trail
    print(f"Inventory updated for material {material_id}: "
          f"Qty={qty_purchased}, Landed Cost={landed_cost_per_unit}, "
          f"New Avg={new_avg if row else landed_cost_per_unit}")
