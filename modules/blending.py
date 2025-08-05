"""
Blending Module for PUVI Oil Manufacturing System
Handles multi-oil blending with dynamic ratios and traceability
"""

from flask import Blueprint, request, jsonify
from decimal import Decimal
from db_utils import get_db_connection, close_connection
from utils.date_utils import parse_date, integer_to_date
from utils.validation import safe_decimal, safe_float, validate_required_fields
from utils.traceability import generate_blend_traceable_code

# Create Blueprint
blending_bp = Blueprint('blending', __name__)

@blending_bp.route('/api/oil_types_for_blending', methods=['GET'])
def get_oil_types_for_blending():
    """Get distinct oil types available for blending"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get oil types from materials (bulk oils)
        cur.execute("""
            SELECT DISTINCT 
                CASE 
                    WHEN m.material_name LIKE '%Groundnut%' THEN 'Groundnut'
                    WHEN m.material_name LIKE '%Sesame%' THEN 'Sesame'
                    WHEN m.material_name LIKE '%Coconut%' THEN 'Coconut'
                    WHEN m.material_name LIKE '%Mustard%' THEN 'Mustard'
                    ELSE SPLIT_PART(m.material_name, ' ', 1)
                END as oil_type
            FROM materials m
            WHERE m.category IN ('Oil', 'Bulk Oil', 'Seeds')
                OR m.material_name LIKE '%Oil%'
            ORDER BY oil_type
        """)
        
        oil_types = [row[0] for row in cur.fetchall() if row[0]]
        
        # Also get from batch production
        cur.execute("""
            SELECT DISTINCT oil_type 
            FROM batch 
            WHERE oil_type IS NOT NULL
            ORDER BY oil_type
        """)
        
        batch_oil_types = [row[0] for row in cur.fetchall() if row[0]]
        
        # Combine and deduplicate
        all_oil_types = list(set(oil_types + batch_oil_types))
        all_oil_types.sort()
        
        return jsonify({
            'success': True,
            'oil_types': all_oil_types
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@blending_bp.route('/api/batches_for_oil_type', methods=['GET'])
def get_batches_for_oil_type():
    """Get available batches for a specific oil type"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        oil_type = request.args.get('oil_type')
        if not oil_type:
            return jsonify({'success': False, 'error': 'Oil type is required'}), 400
        
        batches = []
        
        # 1. Get from batch production (internal extraction)
        cur.execute("""
            SELECT 
                b.batch_id,
                b.batch_code,
                b.oil_type,
                b.production_date,
                COALESCE(i.closing_stock, b.oil_yield - COALESCE(
                    (SELECT SUM(quantity_used) 
                     FROM blend_batch_components 
                     WHERE source_batch_id = b.batch_id 
                     AND source_type = 'extraction'), 0
                )) as available_quantity,
                b.oil_cost_per_kg,
                b.traceable_code,
                'extraction' as source_type
            FROM batch b
            LEFT JOIN inventory i ON i.source_reference_id = b.batch_id 
                AND i.source_type = 'extraction'
                AND i.oil_type = b.oil_type
            WHERE b.oil_type = %s
                AND (i.closing_stock > 0 OR b.oil_yield > COALESCE(
                    (SELECT SUM(quantity_used) 
                     FROM blend_batch_components 
                     WHERE source_batch_id = b.batch_id 
                     AND source_type = 'extraction'), 0
                ))
            ORDER BY b.production_date DESC
        """, (oil_type,))
        
        for row in cur.fetchall():
            batches.append({
                'batch_id': row[0],
                'batch_code': row[1],
                'oil_type': row[2],
                'production_date': integer_to_date(row[3]),
                'available_quantity': float(row[4]),
                'cost_per_kg': float(row[5]),
                'traceable_code': row[6],
                'source_type': row[7],
                'display_name': f"{row[1]} - {integer_to_date(row[3])}"
            })
        
        # 2. Get from previous blends
        cur.execute("""
            SELECT 
                bl.blend_id,
                bl.blend_code,
                %s as oil_type,
                bl.blend_date,
                COALESCE(i.closing_stock, bl.total_quantity - COALESCE(
                    (SELECT SUM(quantity_used) 
                     FROM blend_batch_components 
                     WHERE source_batch_id = bl.blend_id 
                     AND source_type = 'blended'), 0
                )) as available_quantity,
                bl.weighted_avg_cost,
                bl.traceable_code,
                'blended' as source_type
            FROM blend_batches bl
            LEFT JOIN inventory i ON i.source_reference_id = bl.blend_id 
                AND i.source_type = 'blended'
            WHERE EXISTS (
                SELECT 1 FROM blend_batch_components bc
                WHERE bc.blend_id = bl.blend_id
                AND bc.oil_type = %s
            )
            AND (i.closing_stock > 0 OR bl.total_quantity > COALESCE(
                (SELECT SUM(quantity_used) 
                 FROM blend_batch_components 
                 WHERE source_batch_id = bl.blend_id 
                 AND source_type = 'blended'), 0
            ))
            ORDER BY bl.blend_date DESC
        """, (oil_type, oil_type))
        
        for row in cur.fetchall():
            batches.append({
                'batch_id': row[0],
                'batch_code': row[1],
                'oil_type': row[2],
                'production_date': integer_to_date(row[3]),
                'available_quantity': float(row[4]),
                'cost_per_kg': float(row[5]),
                'traceable_code': row[6],
                'source_type': row[7],
                'display_name': f"{row[1]} - {integer_to_date(row[3])}"
            })
        
        # 3. Get from outsourced/purchased bulk oil
        cur.execute("""
            SELECT 
                i.inventory_id,
                COALESCE(p.invoice_ref, 'Outsourced') as batch_code,
                i.oil_type,
                p.purchase_date,
                i.closing_stock as available_quantity,
                i.weighted_avg_cost,
                p.traceable_code,
                'outsourced' as source_type,
                m.material_name
            FROM inventory i
            LEFT JOIN purchases p ON p.purchase_id = i.source_reference_id
            LEFT JOIN materials m ON m.material_id = i.material_id
            WHERE i.oil_type = %s
                AND i.source_type = 'purchase'
                AND i.closing_stock > 0
                AND i.is_bulk_oil = true
            ORDER BY p.purchase_date DESC
        """, (oil_type,))
        
        for row in cur.fetchall():
            batches.append({
                'batch_id': row[0],
                'batch_code': row[1],
                'oil_type': row[2],
                'production_date': integer_to_date(row[3]) if row[3] else 'N/A',
                'available_quantity': float(row[4]),
                'cost_per_kg': float(row[5]),
                'traceable_code': row[6],
                'source_type': row[7],
                'display_name': f"{row[8] or row[1]} - Outsourced"
            })
        
        # Group by source type for better UI organization
        grouped_batches = {
            'extraction': [b for b in batches if b['source_type'] == 'extraction'],
            'blended': [b for b in batches if b['source_type'] == 'blended'],
            'outsourced': [b for b in batches if b['source_type'] == 'outsourced']
        }
        
        return jsonify({
            'success': True,
            'batches': batches,
            'grouped_batches': grouped_batches,
            'total_count': len(batches)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@blending_bp.route('/api/create_blend', methods=['POST'])
def create_blend():
    """Create a new oil blend"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['blend_description', 'blend_date', 'total_quantity', 'components']
        is_valid, missing_fields = validate_required_fields(data, required_fields)
        
        if not is_valid:
            return jsonify({
                'success': False,
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400
        
        # Validate components
        components = data['components']
        if len(components) < 2:
            return jsonify({
                'success': False,
                'error': 'At least 2 components are required for blending'
            }), 400
        
        # Validate percentages sum to 100
        total_percentage = sum(safe_float(c.get('percentage', 0)) for c in components)
        if abs(total_percentage - 100) > 0.01:  # Allow small floating point difference
            return jsonify({
                'success': False,
                'error': f'Percentages must sum to 100%. Current total: {total_percentage}%'
            }), 400
        
        # Parse date
        blend_date = parse_date(data['blend_date'])
        
        # Generate blend code
        oil_types = list(set([c['oil_type'] for c in components]))
        if len(oil_types) <= 3:
            oil_names = '-'.join(oil_types)
        else:
            # Use abbreviations for 4+ oils
            oil_names = '-'.join([o[:3].upper() for o in oil_types])
        
        date_str = data['blend_date'].replace('-', '')
        blend_code = f"BLEND-{date_str}-{oil_names}-{data['blend_description']}"
        
        # Calculate weighted average cost
        total_quantity = safe_decimal(data['total_quantity'])
        total_cost = Decimal('0')
        
        for component in components:
            percentage = safe_decimal(component['percentage']) / 100
            qty_used = total_quantity * percentage
            cost_per_kg = safe_decimal(component['cost_per_kg'])
            total_cost += qty_used * cost_per_kg
        
        weighted_avg_cost = total_cost / total_quantity if total_quantity > 0 else Decimal('0')
        
        # Prepare components for traceable code generation
        blend_components_for_trace = []
        for component in components:
            blend_components_for_trace.append({
                'traceable_code': component.get('traceable_code', ''),
                'percentage': safe_float(component['percentage'])
            })
        
        # Generate traceable code
        # Note: This is a simplified version. The actual implementation might need adjustment
        traceable_code = f"BLEND-{oil_names}-{date_str}"
        
        # Begin transaction
        cur.execute("BEGIN")
        
        # Insert blend master record
        cur.execute("""
            INSERT INTO blend_batches (
                blend_code, blend_description, blend_date,
                total_quantity, weighted_avg_cost, traceable_code,
                created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING blend_id
        """, (
            blend_code,
            data['blend_description'],
            blend_date,
            float(total_quantity),
            float(weighted_avg_cost),
            traceable_code,
            data.get('created_by', 'System')
        ))
        
        blend_id = cur.fetchone()[0]
        
        # Insert blend components and update source inventory
        for component in components:
            oil_type = component['oil_type']
            source_type = component['source_type']
            source_batch_id = component.get('batch_id')
            source_batch_code = component.get('batch_code')
            percentage = safe_decimal(component['percentage']) / 100
            quantity_used = total_quantity * percentage
            cost_per_kg = safe_decimal(component['cost_per_kg'])
            
            # Insert component record
            cur.execute("""
                INSERT INTO blend_batch_components (
                    blend_id, oil_type, source_type, source_batch_id,
                    source_batch_code, quantity_used, percentage,
                    cost_per_unit, traceable_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                blend_id,
                oil_type,
                source_type,
                source_batch_id,
                source_batch_code,
                float(quantity_used),
                float(component['percentage']),
                float(cost_per_kg),
                component.get('traceable_code')
            ))
            
            # Update source inventory (deduct quantity)
            if source_type == 'extraction':
                # Update inventory for batch production
                cur.execute("""
                    UPDATE inventory
                    SET closing_stock = closing_stock - %s,
                        consumption = consumption + %s,
                        last_updated = %s
                    WHERE source_reference_id = %s
                        AND source_type = 'extraction'
                        AND oil_type = %s
                        AND closing_stock >= %s
                """, (
                    float(quantity_used),
                    float(quantity_used),
                    blend_date,
                    source_batch_id,
                    oil_type,
                    float(quantity_used)
                ))
                
            elif source_type == 'blended':
                # Update inventory for previous blend
                cur.execute("""
                    UPDATE inventory
                    SET closing_stock = closing_stock - %s,
                        consumption = consumption + %s,
                        last_updated = %s
                    WHERE source_reference_id = %s
                        AND source_type = 'blended'
                        AND closing_stock >= %s
                """, (
                    float(quantity_used),
                    float(quantity_used),
                    blend_date,
                    source_batch_id,
                    float(quantity_used)
                ))
                
            elif source_type == 'outsourced':
                # Update inventory for purchased oil
                cur.execute("""
                    UPDATE inventory
                    SET closing_stock = closing_stock - %s,
                        consumption = consumption + %s,
                        last_updated = %s
                    WHERE inventory_id = %s
                        AND closing_stock >= %s
                """, (
                    float(quantity_used),
                    float(quantity_used),
                    blend_date,
                    source_batch_id,  # For outsourced, batch_id is inventory_id
                    float(quantity_used)
                ))
        
        # Create inventory record for the new blend
        cur.execute("""
            INSERT INTO inventory (
                oil_type, closing_stock, weighted_avg_cost,
                last_updated, source_type, source_reference_id,
                is_bulk_oil, opening_stock
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            oil_names if len(oil_types) == 1 else 'Mixed',
            float(total_quantity),
            float(weighted_avg_cost),
            blend_date,
            'blended',
            blend_id,
            True,
            float(total_quantity)
        ))
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'blend_id': blend_id,
            'blend_code': blend_code,
            'traceable_code': traceable_code,
            'weighted_avg_cost': float(weighted_avg_cost),
            'message': f'Blend {blend_code} created successfully!'
        }), 201
        
    except Exception as e:
        conn.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@blending_bp.route('/api/blend_history', methods=['GET'])
def get_blend_history():
    """Get blend history with component details"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get query parameters
        limit = request.args.get('limit', 50, type=int)
        oil_type = request.args.get('oil_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Build query
        query = """
            SELECT 
                bl.blend_id,
                bl.blend_code,
                bl.blend_description,
                bl.blend_date,
                bl.total_quantity,
                bl.weighted_avg_cost,
                bl.traceable_code,
                bl.created_by,
                bl.created_at,
                COUNT(DISTINCT bc.component_id) as component_count,
                STRING_AGG(DISTINCT bc.oil_type, ', ') as oil_types
            FROM blend_batches bl
            LEFT JOIN blend_batch_components bc ON bl.blend_id = bc.blend_id
            WHERE 1=1
        """
        
        params = []
        
        if oil_type:
            query += """ AND EXISTS (
                SELECT 1 FROM blend_batch_components bc2 
                WHERE bc2.blend_id = bl.blend_id 
                AND bc2.oil_type = %s
            )"""
            params.append(oil_type)
        
        if start_date:
            query += " AND bl.blend_date >= %s"
            params.append(parse_date(start_date))
        
        if end_date:
            query += " AND bl.blend_date <= %s"
            params.append(parse_date(end_date))
        
        query += """
            GROUP BY bl.blend_id, bl.blend_code, bl.blend_description,
                     bl.blend_date, bl.total_quantity, bl.weighted_avg_cost,
                     bl.traceable_code, bl.created_by, bl.created_at
            ORDER BY bl.blend_date DESC, bl.blend_id DESC
            LIMIT %s
        """
        params.append(limit)
        
        cur.execute(query, params)
        
        blends = []
        for row in cur.fetchall():
            blend = {
                'blend_id': row[0],
                'blend_code': row[1],
                'blend_description': row[2],
                'blend_date': integer_to_date(row[3]),
                'total_quantity': float(row[4]),
                'weighted_avg_cost': float(row[5]),
                'traceable_code': row[6],
                'created_by': row[7],
                'created_at': row[8].isoformat() if row[8] else None,
                'component_count': row[9],
                'oil_types': row[10]
            }
            
            # Get components for this blend
            cur.execute("""
                SELECT 
                    oil_type,
                    source_type,
                    source_batch_code,
                    quantity_used,
                    percentage,
                    cost_per_unit,
                    traceable_code
                FROM blend_batch_components
                WHERE blend_id = %s
                ORDER BY percentage DESC
            """, (row[0],))
            
            components = []
            for comp_row in cur.fetchall():
                components.append({
                    'oil_type': comp_row[0],
                    'source_type': comp_row[1],
                    'source_batch_code': comp_row[2],
                    'quantity_used': float(comp_row[3]),
                    'percentage': float(comp_row[4]),
                    'cost_per_unit': float(comp_row[5]),
                    'traceable_code': comp_row[6]
                })
            
            blend['components'] = components
            blends.append(blend)
        
        # Get summary statistics
        summary_query = """
            SELECT 
                COUNT(*) as total_blends,
                COALESCE(SUM(total_quantity), 0) as total_quantity_blended,
                COALESCE(AVG(weighted_avg_cost), 0) as avg_blend_cost
            FROM blend_batches
            WHERE 1=1
        """
        
        # Apply same filters for summary
        summary_params = params[:-1]  # Exclude limit
        if oil_type:
            summary_query += """ AND EXISTS (
                SELECT 1 FROM blend_batch_components bc 
                WHERE bc.blend_id = blend_batches.blend_id 
                AND bc.oil_type = %s
            )"""
        if start_date:
            summary_query += " AND blend_date >= %s"
        if end_date:
            summary_query += " AND blend_date <= %s"
        
        cur.execute(summary_query, summary_params)
        stats = cur.fetchone()
        
        return jsonify({
            'success': True,
            'blends': blends,
            'count': len(blends),
            'summary': {
                'total_blends': stats[0],
                'total_quantity_blended': float(stats[1]),
                'avg_blend_cost': float(stats[2])
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)
