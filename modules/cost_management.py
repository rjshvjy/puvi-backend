"""
Cost Management Module for PUVI Oil Manufacturing System
Handles all cost elements, time tracking, and Phase 1 validation (warnings only)
File Path: puvi-backend/modules/cost_management.py
"""

from flask import Blueprint, request, jsonify
from decimal import Decimal
from datetime import datetime, timedelta
from db_utils import get_db_connection, close_connection
from utils.date_utils import parse_date, integer_to_date
from utils.validation import safe_decimal, safe_float

# Create Blueprint
cost_management_bp = Blueprint('cost_management', __name__)

class CostValidationWarning:
    """Class to handle cost validation warnings (Phase 1)"""
    def __init__(self):
        self.warnings = []
        self.unallocated_costs = Decimal('0')
        
    def add_warning(self, message, amount=None):
        """Add a warning message"""
        warning = {
            'message': message,
            'amount': float(amount) if amount else None,
            'type': 'warning'
        }
        self.warnings.append(warning)
        if amount:
            self.unallocated_costs += Decimal(str(amount))
    
    def get_summary(self):
        """Get validation summary"""
        return {
            'has_warnings': len(self.warnings) > 0,
            'warning_count': len(self.warnings),
            'warnings': self.warnings,
            'total_unallocated': float(self.unallocated_costs)
        }


@cost_management_bp.route('/api/cost_elements/master', methods=['GET'])
def get_cost_elements_master():
    """Get all active cost elements with their default rates"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get applicable_to filter if provided
        applicable_to = request.args.get('applicable_to', 'all')
        
        if applicable_to == 'all':
            query = """
                SELECT 
                    element_id,
                    element_name,
                    category,
                    unit_type,
                    default_rate,
                    calculation_method,
                    is_optional,
                    applicable_to,
                    display_order
                FROM cost_elements_master
                WHERE active = true
                ORDER BY display_order, category, element_name
            """
            cur.execute(query)
        else:
            query = """
                SELECT 
                    element_id,
                    element_name,
                    category,
                    unit_type,
                    default_rate,
                    calculation_method,
                    is_optional,
                    applicable_to,
                    display_order
                FROM cost_elements_master
                WHERE active = true 
                    AND applicable_to IN (%s, 'all')
                ORDER BY display_order, category, element_name
            """
            cur.execute(query, (applicable_to,))
        
        cost_elements = []
        for row in cur.fetchall():
            cost_elements.append({
                'element_id': row[0],
                'element_name': row[1],
                'category': row[2],
                'unit_type': row[3],
                'default_rate': float(row[4]),
                'calculation_method': row[5],
                'is_optional': row[6],
                'applicable_to': row[7],
                'display_order': row[8]
            })
        
        # Group by category for easier UI rendering
        by_category = {}
        for element in cost_elements:
            category = element['category']
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(element)
        
        return jsonify({
            'success': True,
            'cost_elements': cost_elements,
            'by_category': by_category,
            'count': len(cost_elements)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@cost_management_bp.route('/api/cost_elements/by_stage', methods=['GET'])
def get_cost_elements_by_stage():
    """Get cost elements applicable to a specific production stage"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        stage = request.args.get('stage', 'batch')  # batch, purchase, sales
        
        cur.execute("""
            SELECT 
                element_id,
                element_name,
                category,
                unit_type,
                default_rate,
                calculation_method,
                is_optional
            FROM cost_elements_master
            WHERE active = true 
                AND applicable_to IN (%s, 'all')
            ORDER BY display_order
        """, (stage,))
        
        cost_elements = []
        for row in cur.fetchall():
            cost_elements.append({
                'element_id': row[0],
                'element_name': row[1],
                'category': row[2],
                'unit_type': row[3],
                'default_rate': float(row[4]),
                'calculation_method': row[5],
                'is_optional': row[6]
            })
        
        return jsonify({
            'success': True,
            'stage': stage,
            'cost_elements': cost_elements,
            'count': len(cost_elements)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@cost_management_bp.route('/api/cost_elements/time_tracking', methods=['POST'])
def save_time_tracking():
    """Save time tracking data for a batch with user-entered times"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        
        # Required fields
        batch_id = data.get('batch_id')
        process_type = data.get('process_type', 'crushing')
        start_datetime = data.get('start_datetime')  # Format: "2025-08-06 10:30"
        end_datetime = data.get('end_datetime')      # Format: "2025-08-06 15:45"
        
        if not all([batch_id, start_datetime, end_datetime]):
            return jsonify({
                'success': False,
                'error': 'batch_id, start_datetime, and end_datetime are required'
            }), 400
        
        # Parse datetime strings
        start_dt = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M')
        end_dt = datetime.strptime(end_datetime, '%Y-%m-%d %H:%M')
        
        # Validate end time is after start time
        if end_dt <= start_dt:
            return jsonify({
                'success': False,
                'error': 'End time must be after start time'
            }), 400
        
        # Calculate duration
        duration = end_dt - start_dt
        total_hours = Decimal(str(duration.total_seconds() / 3600))
        rounded_hours = int(total_hours.quantize(Decimal('1'), rounding='ROUND_UP'))
        
        # Begin transaction
        cur.execute("BEGIN")
        
        # Insert time tracking record
        cur.execute("""
            INSERT INTO batch_time_tracking (
                batch_id, process_type, start_datetime, end_datetime,
                total_hours, rounded_hours, operator_name, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING tracking_id
        """, (
            batch_id,
            process_type,
            start_dt,
            end_dt,
            float(total_hours),
            rounded_hours,
            data.get('operator_name', ''),
            data.get('notes', '')
        ))
        
        tracking_id = cur.fetchone()[0]
        
        # Calculate time-based costs automatically
        time_costs = calculate_time_based_costs(cur, rounded_hours)
        
        # Save time-based costs to batch_extended_costs
        for cost in time_costs:
            cur.execute("""
                INSERT INTO batch_extended_costs (
                    batch_id, element_id, element_name,
                    quantity_or_hours, rate_used, total_cost,
                    is_applied, created_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                cost['element_id'],
                cost['element_name'],
                rounded_hours,
                cost['rate'],
                cost['total_cost'],
                True,
                data.get('created_by', 'System')
            ))
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'tracking_id': tracking_id,
            'total_hours': float(total_hours),
            'rounded_hours': rounded_hours,
            'time_costs': time_costs,
            'total_time_cost': sum(c['total_cost'] for c in time_costs),
            'message': f'Time tracking saved: {total_hours:.2f} hours (billed as {rounded_hours} hours)'
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@cost_management_bp.route('/api/cost_elements/calculate', methods=['POST'])
def calculate_batch_costs():
    """Calculate all costs for a batch with Phase 1 validation warnings"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        batch_id = data.get('batch_id')
        
        if not batch_id:
            return jsonify({'success': False, 'error': 'batch_id is required'}), 400
        
        # Initialize validation warnings
        validator = CostValidationWarning()
        
        # Get batch details
        cur.execute("""
            SELECT 
                batch_code, oil_type, seed_quantity_before_drying,
                seed_quantity_after_drying, oil_yield, oil_cake_yield,
                sludge_yield, total_production_cost
            FROM batch
            WHERE batch_id = %s
        """, (batch_id,))
        
        batch = cur.fetchone()
        if not batch:
            return jsonify({'success': False, 'error': 'Batch not found'}), 404
        
        batch_data = {
            'batch_code': batch[0],
            'oil_type': batch[1],
            'seed_qty_before': float(batch[2]),
            'seed_qty_after': float(batch[3]),
            'oil_yield': float(batch[4]),
            'cake_yield': float(batch[5]),
            'sludge_yield': float(batch[6]) if batch[6] else 0,
            'base_production_cost': float(batch[7])
        }
        
        # Get all applicable cost elements
        cur.execute("""
            SELECT 
                element_id,
                element_name,
                category,
                unit_type,
                default_rate,
                calculation_method,
                is_optional
            FROM cost_elements_master
            WHERE active = true 
                AND applicable_to IN ('batch', 'all')
            ORDER BY display_order
        """)
        
        cost_elements = cur.fetchall()
        cost_breakdown = []
        total_extended_costs = Decimal('0')
        
        # Check for time tracking
        cur.execute("""
            SELECT SUM(rounded_hours) as total_hours
            FROM batch_time_tracking
            WHERE batch_id = %s
        """, (batch_id,))
        
        time_result = cur.fetchone()
        total_hours = time_result[0] if time_result[0] else 0
        
        # Process each cost element
        for element in cost_elements:
            element_id, element_name, category, unit_type, default_rate, calc_method, is_optional = element
            
            # Check if this cost has been captured
            cur.execute("""
                SELECT quantity_or_hours, rate_used, total_cost
                FROM batch_extended_costs
                WHERE batch_id = %s AND element_id = %s
            """, (batch_id, element_id))
            
            existing_cost = cur.fetchone()
            
            if calc_method == 'per_hour':
                if total_hours > 0:
                    if not existing_cost:
                        # Time tracked but cost not calculated - WARNING
                        cost = float(total_hours) * float(default_rate)
                        validator.add_warning(
                            f"{element_name}: {total_hours} hours tracked but cost not recorded (₹{cost:.2f})",
                            cost
                        )
                else:
                    # No time tracking - WARNING
                    validator.add_warning(f"{element_name}: No time tracking recorded")
                    
            elif calc_method == 'per_kg':
                expected_cost = float(batch_data['seed_qty_before']) * float(default_rate)
                if not existing_cost and not is_optional:
                    validator.add_warning(
                        f"{element_name}: Not recorded (Expected: ₹{expected_cost:.2f})",
                        expected_cost
                    )
                    
            elif calc_method == 'fixed':
                if not existing_cost and not is_optional:
                    validator.add_warning(
                        f"{element_name}: Fixed cost not recorded (₹{default_rate})",
                        default_rate
                    )
            
            # Add to breakdown if exists
            if existing_cost:
                cost_breakdown.append({
                    'element_name': element_name,
                    'category': category,
                    'quantity': float(existing_cost[0]),
                    'rate': float(existing_cost[1]),
                    'total_cost': float(existing_cost[2])
                })
                total_extended_costs += Decimal(str(existing_cost[2]))
        
        # Check for common costs allocation
        cur.execute("""
            SELECT SUM(total_cost) 
            FROM batch_extended_costs bec
            JOIN cost_elements_master cem ON bec.element_id = cem.element_id
            WHERE bec.batch_id = %s AND cem.element_name = 'Common Costs'
        """, (batch_id,))
        
        common_costs_result = cur.fetchone()
        if not common_costs_result or not common_costs_result[0]:
            # Common costs not allocated
            expected_common = float(batch_data['oil_yield']) * 2.0  # ₹2/kg
            validator.add_warning(
                f"Common Costs: Not allocated to this batch (₹{expected_common:.2f} @ ₹2/kg)",
                expected_common
            )
        
        # Calculate total costs
        total_costs = batch_data['base_production_cost'] + float(total_extended_costs)
        
        # Get validation summary
        validation = validator.get_summary()
        
        # Calculate oil cost per kg
        if batch_data['oil_yield'] > 0:
            oil_cost_per_kg = total_costs / batch_data['oil_yield']
        else:
            oil_cost_per_kg = 0
        
        return jsonify({
            'success': True,
            'batch_code': batch_data['batch_code'],
            'cost_breakdown': cost_breakdown,
            'base_production_cost': batch_data['base_production_cost'],
            'extended_costs': float(total_extended_costs),
            'total_costs': total_costs,
            'oil_yield': batch_data['oil_yield'],
            'oil_cost_per_kg': oil_cost_per_kg,
            'validation': validation,
            'message': 'Cost calculation complete' + 
                      (f' with {validation["warning_count"]} warnings' if validation['has_warnings'] else '')
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@cost_management_bp.route('/api/cost_elements/save_batch_costs', methods=['POST'])
def save_batch_costs():
    """Save extended costs for a batch with override capability"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        data = request.json
        batch_id = data.get('batch_id')
        costs = data.get('costs', [])
        created_by = data.get('created_by', 'System')
        
        if not batch_id:
            return jsonify({'success': False, 'error': 'batch_id is required'}), 400
        
        # Begin transaction
        cur.execute("BEGIN")
        
        saved_costs = []
        total_saved = Decimal('0')
        
        for cost_item in costs:
            element_id = cost_item.get('element_id')
            element_name = cost_item.get('element_name')
            quantity = safe_decimal(cost_item.get('quantity', 0))
            rate = safe_decimal(cost_item.get('rate', 0))
            override_rate = cost_item.get('override_rate')
            is_applied = cost_item.get('is_applied', True)
            
            # Use override rate if provided
            if override_rate is not None:
                actual_rate = safe_decimal(override_rate)
                
                # Log the override
                cur.execute("""
                    INSERT INTO cost_override_log (
                        module_name, record_id, element_id, element_name,
                        original_rate, override_rate, reason, overridden_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'batch',
                    batch_id,
                    element_id,
                    element_name,
                    float(rate),
                    float(actual_rate),
                    cost_item.get('override_reason', 'Manual adjustment'),
                    created_by
                ))
            else:
                actual_rate = rate
            
            # Calculate total cost
            total_cost = quantity * actual_rate
            
            # Check if this cost already exists
            cur.execute("""
                SELECT cost_id FROM batch_extended_costs
                WHERE batch_id = %s AND element_id = %s
            """, (batch_id, element_id))
            
            existing = cur.fetchone()
            
            if existing:
                # Update existing
                cur.execute("""
                    UPDATE batch_extended_costs
                    SET quantity_or_hours = %s,
                        rate_used = %s,
                        total_cost = %s,
                        is_applied = %s,
                        created_by = %s
                    WHERE cost_id = %s
                """, (
                    float(quantity),
                    float(actual_rate),
                    float(total_cost),
                    is_applied,
                    created_by,
                    existing[0]
                ))
            else:
                # Insert new
                cur.execute("""
                    INSERT INTO batch_extended_costs (
                        batch_id, element_id, element_name,
                        quantity_or_hours, rate_used, total_cost,
                        is_applied, created_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    batch_id,
                    element_id,
                    element_name,
                    float(quantity),
                    float(actual_rate),
                    float(total_cost),
                    is_applied,
                    created_by
                ))
            
            if is_applied:
                saved_costs.append({
                    'element_name': element_name,
                    'quantity': float(quantity),
                    'rate': float(actual_rate),
                    'total_cost': float(total_cost)
                })
                total_saved += total_cost
        
        # Commit transaction
        conn.commit()
        
        return jsonify({
            'success': True,
            'batch_id': batch_id,
            'saved_costs': saved_costs,
            'total_extended_costs': float(total_saved),
            'message': f'{len(saved_costs)} cost elements saved successfully'
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


@cost_management_bp.route('/api/cost_elements/batch_summary/<int:batch_id>', methods=['GET'])
def get_batch_cost_summary(batch_id):
    """Get complete cost summary for a batch with validation warnings"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get batch basic info
        cur.execute("""
            SELECT 
                b.batch_code,
                b.oil_type,
                b.production_date,
                b.seed_quantity_before_drying,
                b.oil_yield,
                b.oil_cake_yield,
                b.total_production_cost,
                b.net_oil_cost,
                b.oil_cost_per_kg,
                b.cake_estimated_rate,
                b.cake_actual_rate
            FROM batch b
            WHERE b.batch_id = %s
        """, (batch_id,))
        
        batch = cur.fetchone()
        if not batch:
            return jsonify({'success': False, 'error': 'Batch not found'}), 404
        
        # Get extended costs
        cur.execute("""
            SELECT 
                bec.element_name,
                cem.category,
                bec.quantity_or_hours,
                bec.rate_used,
                bec.total_cost,
                bec.is_applied
            FROM batch_extended_costs bec
            LEFT JOIN cost_elements_master cem ON bec.element_id = cem.element_id
            WHERE bec.batch_id = %s
            ORDER BY cem.display_order
        """, (batch_id,))
        
        extended_costs = []
        total_extended = Decimal('0')
        
        for row in cur.fetchall():
            extended_costs.append({
                'element_name': row[0],
                'category': row[1],
                'quantity': float(row[2]),
                'rate': float(row[3]),
                'total_cost': float(row[4]),
                'is_applied': row[5]
            })
            if row[5]:  # If applied
                total_extended += Decimal(str(row[4]))
        
        # Get time tracking
        cur.execute("""
            SELECT 
                process_type,
                start_datetime,
                end_datetime,
                total_hours,
                rounded_hours
            FROM batch_time_tracking
            WHERE batch_id = %s
            ORDER BY start_datetime
        """, (batch_id,))
        
        time_tracking = []
        for row in cur.fetchall():
            time_tracking.append({
                'process_type': row[0],
                'start_time': row[1].strftime('%Y-%m-%d %H:%M') if row[1] else None,
                'end_time': row[2].strftime('%Y-%m-%d %H:%M') if row[2] else None,
                'actual_hours': float(row[3]) if row[3] else 0,
                'billed_hours': row[4] if row[4] else 0
            })
        
        # Run validation check
        validator = CostValidationWarning()
        
        # Check for missing costs
        cur.execute("""
            SELECT element_name, default_rate, calculation_method, is_optional
            FROM cost_elements_master
            WHERE active = true 
                AND applicable_to IN ('batch', 'all')
                AND element_id NOT IN (
                    SELECT element_id FROM batch_extended_costs WHERE batch_id = %s
                )
        """, (batch_id,))
        
        for missing in cur.fetchall():
            if not missing[3]:  # If not optional
                validator.add_warning(f"{missing[0]}: Not captured (Default: ₹{missing[1]})")
        
        # Prepare summary
        summary = {
            'batch_code': batch[0],
            'oil_type': batch[1],
            'production_date': integer_to_date(batch[2]),
            'seed_quantity': float(batch[3]),
            'oil_yield': float(batch[4]),
            'cake_yield': float(batch[5]),
            'base_production_cost': float(batch[6]),
            'extended_costs': extended_costs,
            'total_extended_costs': float(total_extended),
            'total_production_cost': float(batch[6]) + float(total_extended),
            'net_oil_cost': float(batch[7]),
            'oil_cost_per_kg': float(batch[8]),
            'cake_estimated_rate': float(batch[9]) if batch[9] else 0,
            'cake_actual_rate': float(batch[10]) if batch[10] else 0,
            'time_tracking': time_tracking,
            'validation': validator.get_summary()
        }
        
        return jsonify({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)


# Helper Functions
def calculate_time_based_costs(cur, hours):
    """Calculate costs for time-based elements"""
    cur.execute("""
        SELECT 
            element_id,
            element_name,
            default_rate
        FROM cost_elements_master
        WHERE calculation_method = 'per_hour'
            AND applicable_to IN ('batch', 'all')
            AND active = true
    """)
    
    costs = []
    for row in cur.fetchall():
        costs.append({
            'element_id': row[0],
            'element_name': row[1],
            'rate': float(row[2]),
            'hours': hours,
            'total_cost': float(row[2]) * hours
        })
    
    return costs


@cost_management_bp.route('/api/cost_elements/validation_report', methods=['GET'])
def get_validation_report():
    """Get validation report for all recent batches (Phase 1 - Warnings only)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get date range from parameters
        days = request.args.get('days', 30, type=int)
        
        cur.execute("""
            SELECT 
                b.batch_id,
                b.batch_code,
                b.oil_type,
                b.production_date,
                COUNT(DISTINCT bec.element_id) as costs_captured,
                COUNT(DISTINCT cem.element_id) as costs_expected
            FROM batch b
            CROSS JOIN cost_elements_master cem
            LEFT JOIN batch_extended_costs bec 
                ON b.batch_id = bec.batch_id 
                AND bec.element_id = cem.element_id
            WHERE cem.active = true 
                AND cem.applicable_to IN ('batch', 'all')
                AND cem.is_optional = false
                AND b.production_date >= (
                    SELECT MAX(production_date) - %s FROM batch
                )
            GROUP BY b.batch_id, b.batch_code, b.oil_type, b.production_date
            HAVING COUNT(DISTINCT bec.element_id) < COUNT(DISTINCT cem.element_id)
            ORDER BY b.production_date DESC
        """, (days,))
        
        batches_with_warnings = []
        for row in cur.fetchall():
            batches_with_warnings.append({
                'batch_id': row[0],
                'batch_code': row[1],
                'oil_type': row[2],
                'production_date': integer_to_date(row[3]),
                'costs_captured': row[4],
                'costs_expected': row[5],
                'missing_count': row[5] - row[4]
            })
        
        return jsonify({
            'success': True,
            'report_period_days': days,
            'batches_with_warnings': batches_with_warnings,
            'total_batches_with_warnings': len(batches_with_warnings),
            'message': 'Phase 1 Validation - Warnings only, operations not blocked'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        close_connection(conn, cur)
