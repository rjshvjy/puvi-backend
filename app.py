"""
Main Flask Application for PUVI Oil Manufacturing System
Integrates all modules including Cost Management
File Path: puvi-backend/app.py
"""

from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime
from db_utils import get_db_connection, close_connection

# Import all module blueprints
from modules.purchase import purchase_bp
from modules.material_writeoff import writeoff_bp
from modules.batch_production import batch_bp
from modules.blending import blending_bp
from modules.material_sales import material_sales_bp
from modules.cost_management import cost_management_bp  # NEW - Import cost management module

# Create Flask app
app = Flask(__name__)

# Enable CORS for all routes - Updated to handle all Vercel URLs
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "http://localhost:3000",
            "http://localhost:3001",
            "https://puvi-frontend.vercel.app",
            "https://puvi-frontend-*.vercel.app",
            "https://*.vercel.app",  # This will catch all Vercel preview URLs
            "https://puvi-frontend-740w3x6v2-rajeshs-projects-8be31e4e.vercel.app"  # Your specific URL
        ],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# Register all blueprints
app.register_blueprint(purchase_bp)
app.register_blueprint(writeoff_bp)
app.register_blueprint(batch_bp)
app.register_blueprint(blending_bp)
app.register_blueprint(material_sales_bp)
app.register_blueprint(cost_management_bp)  # NEW - Register cost management blueprint

# Configuration
app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Root endpoint
@app.route('/', methods=['GET'])
def home():
    """Root endpoint to verify API is running"""
    return jsonify({
        'status': 'PUVI Backend API is running!',
        'version': '7.0',  # Updated version with Cost Management
        'timestamp': datetime.now().isoformat(),
        'endpoints': {
            'health': '/api/health',
            'modules': {
                'purchase': [
                    '/api/materials',
                    '/api/add_purchase',
                    '/api/purchase_history',
                    '/api/suppliers'
                ],
                'writeoff': [
                    '/api/writeoff_reasons',
                    '/api/inventory_for_writeoff',
                    '/api/add_writeoff',
                    '/api/writeoff_history'
                ],
                'batch': [
                    '/api/seeds_for_batch',
                    '/api/cost_elements_for_batch',
                    '/api/oil_cake_rates',
                    '/api/add_batch',
                    '/api/batch_history'
                ],
                'blending': [
                    '/api/oil_types_for_blending',
                    '/api/batches_for_oil_type',
                    '/api/create_blend',
                    '/api/blend_history'
                ],
                'material_sales': [
                    '/api/byproduct_types',
                    '/api/material_sales_inventory',
                    '/api/add_material_sale',
                    '/api/material_sales_history',
                    '/api/cost_reconciliation_report'
                ],
                'cost_management': [  # NEW - Cost management endpoints
                    '/api/cost_elements/master',
                    '/api/cost_elements/by_stage',
                    '/api/cost_elements/time_tracking',
                    '/api/cost_elements/calculate',
                    '/api/cost_elements/save_batch_costs',
                    '/api/cost_elements/batch_summary/<batch_id>',
                    '/api/cost_elements/validation_report'
                ]
            }
        }
    })

# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint with database connectivity test"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get various counts to verify database
        queries = {
            'materials': "SELECT COUNT(*) FROM materials",
            'purchases': "SELECT COUNT(*) FROM purchases",
            'batches': "SELECT COUNT(*) FROM batch",
            'writeoffs': "SELECT COUNT(*) FROM material_writeoffs",
            'blends': "SELECT COUNT(*) FROM blend_batches",
            'material_sales': "SELECT COUNT(*) FROM oil_cake_sales",
            'cost_elements': "SELECT COUNT(*) FROM cost_elements_master",  # NEW - Count cost elements
            'time_tracking': "SELECT COUNT(*) FROM batch_time_tracking",   # NEW - Count time tracking
            'inventory_items': "SELECT COUNT(*) FROM inventory WHERE closing_stock > 0"
        }
        
        counts = {}
        for key, query in queries.items():
            try:
                cur.execute(query)
                counts[key] = cur.fetchone()[0]
            except:
                counts[key] = 0  # Table might not exist yet
        
        # Get database size
        cur.execute("""
            SELECT pg_database_size(current_database()) as size
        """)
        db_size = cur.fetchone()[0]
        
        # Get cost validation warnings count (NEW)
        try:
            cur.execute("""
                SELECT COUNT(DISTINCT b.batch_id)
                FROM batch b
                WHERE NOT EXISTS (
                    SELECT 1 FROM batch_extended_costs bec
                    WHERE bec.batch_id = b.batch_id
                )
                AND b.production_date >= (
                    SELECT MAX(production_date) - 30 FROM batch
                )
            """)
            validation_warnings = cur.fetchone()[0]
        except:
            validation_warnings = 0
        
        # Get active modules
        active_modules = []
        for rule in app.url_map.iter_rules():
            if '/api/' in rule.rule:
                module = rule.rule.split('/')[2] if len(rule.rule.split('/')) > 2 else 'core'
                if module not in active_modules and module != 'health':
                    active_modules.append(module)
        
        close_connection(conn, cur)
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'version': '7.0',
            'counts': counts,
            'database_size_mb': round(db_size / 1024 / 1024, 2),
            'active_modules': sorted(active_modules),
            'cost_validation_warnings': validation_warnings,  # NEW - Show validation warnings
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'message': 'The requested endpoint does not exist'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'message': 'An unexpected error occurred'
    }), 500

@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 errors"""
    return jsonify({
        'success': False,
        'error': 'Method not allowed',
        'message': 'The HTTP method is not allowed for this endpoint'
    }), 405

# Utility endpoints
@app.route('/api/system_info', methods=['GET'])
def system_info():
    """Get system information and statistics"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get various system statistics
        stats = {}
        
        # Material statistics
        cur.execute("""
            SELECT 
                COUNT(DISTINCT category) as categories,
                COUNT(*) as total_materials,
                COALESCE(AVG(current_cost), 0) as avg_cost
            FROM materials
        """)
        row = cur.fetchone()
        stats['materials'] = {
            'categories': row[0],
            'total_materials': row[1],
            'average_cost': float(row[2])
        }
        
        # Inventory value
        cur.execute("""
            SELECT 
                COALESCE(SUM(closing_stock * weighted_avg_cost), 0) as total_value,
                COUNT(*) as items_in_stock
            FROM inventory
            WHERE closing_stock > 0
        """)
        row = cur.fetchone()
        stats['inventory'] = {
            'total_value': float(row[0]),
            'items_in_stock': row[1]
        }
        
        # Production statistics
        cur.execute("""
            SELECT 
                COUNT(DISTINCT oil_type) as oil_types,
                COALESCE(SUM(oil_yield), 0) as total_oil_produced,
                COALESCE(SUM(oil_cake_yield), 0) as total_cake_produced,
                COALESCE(AVG(oil_yield_percent), 0) as avg_oil_yield
            FROM batch
        """)
        row = cur.fetchone()
        stats['production'] = {
            'oil_types': row[0],
            'total_oil_produced': float(row[1]),
            'total_cake_produced': float(row[2]),
            'average_oil_yield': float(row[3])
        }
        
        # Blending statistics
        try:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_blends,
                    COALESCE(SUM(total_quantity), 0) as total_blended,
                    COALESCE(AVG(weighted_avg_cost), 0) as avg_blend_cost
                FROM blend_batches
            """)
            row = cur.fetchone()
            stats['blending'] = {
                'total_blends': row[0],
                'total_quantity_blended': float(row[1]),
                'average_blend_cost': float(row[2])
            }
        except:
            stats['blending'] = {
                'total_blends': 0,
                'total_quantity_blended': 0,
                'average_blend_cost': 0
            }
        
        # Material Sales statistics
        try:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_sales,
                    COALESCE(SUM(quantity_sold), 0) as total_quantity_sold,
                    COALESCE(SUM(total_amount), 0) as total_revenue,
                    COUNT(DISTINCT buyer_name) as unique_buyers
                FROM oil_cake_sales
            """)
            row = cur.fetchone()
            stats['material_sales'] = {
                'total_sales': row[0],
                'total_quantity_sold': float(row[1]),
                'total_revenue': float(row[2]),
                'unique_buyers': row[3]
            }
            
            # Get cost adjustments
            cur.execute("""
                SELECT COALESCE(SUM(oil_cost_adjustment), 0) as total_adjustments
                FROM oil_cake_sale_allocations
            """)
            adjustment = cur.fetchone()
            stats['material_sales']['total_cost_adjustments'] = float(adjustment[0])
        except:
            stats['material_sales'] = {
                'total_sales': 0,
                'total_quantity_sold': 0,
                'total_revenue': 0,
                'unique_buyers': 0,
                'total_cost_adjustments': 0
            }
        
        # Cost Management statistics (NEW)
        try:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_cost_elements,
                    COUNT(DISTINCT category) as cost_categories
                FROM cost_elements_master
                WHERE active = true
            """)
            row = cur.fetchone()
            
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT batch_id) as batches_with_extended_costs,
                    COALESCE(SUM(total_cost), 0) as total_extended_costs
                FROM batch_extended_costs
                WHERE is_applied = true
            """)
            extended = cur.fetchone()
            
            cur.execute("""
                SELECT COUNT(*) as time_tracking_entries
                FROM batch_time_tracking
            """)
            time_entries = cur.fetchone()
            
            stats['cost_management'] = {
                'total_cost_elements': row[0],
                'cost_categories': row[1],
                'batches_with_extended_costs': extended[0],
                'total_extended_costs': float(extended[1]),
                'time_tracking_entries': time_entries[0]
            }
        except:
            stats['cost_management'] = {
                'total_cost_elements': 0,
                'cost_categories': 0,
                'batches_with_extended_costs': 0,
                'total_extended_costs': 0,
                'time_tracking_entries': 0
            }
        
        # Writeoff statistics
        cur.execute("""
            SELECT 
                COALESCE(SUM(net_loss), 0) as total_loss,
                COUNT(*) as total_writeoffs
            FROM material_writeoffs
        """)
        row = cur.fetchone()
        stats['writeoffs'] = {
            'total_loss': float(row[0]),
            'total_writeoffs': row[1]
        }
        
        close_connection(conn, cur)
        
        return jsonify({
            'success': True,
            'statistics': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        close_connection(conn, cur)
        return jsonify({'success': False, 'error': str(e)}), 500

# Cost validation endpoint (NEW)
@app.route('/api/cost_validation_summary', methods=['GET'])
def cost_validation_summary():
    """Get summary of cost validation issues across all batches"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get batches with missing costs
        cur.execute("""
            SELECT 
                b.batch_id,
                b.batch_code,
                b.oil_type,
                b.production_date,
                b.oil_yield,
                b.total_production_cost,
                COUNT(bec.cost_id) as extended_costs_count,
                COALESCE(SUM(bec.total_cost), 0) as total_extended_costs
            FROM batch b
            LEFT JOIN batch_extended_costs bec ON b.batch_id = bec.batch_id
            WHERE b.production_date >= (
                SELECT MAX(production_date) - 30 FROM batch
            )
            GROUP BY b.batch_id, b.batch_code, b.oil_type, b.production_date,
                     b.oil_yield, b.total_production_cost
            ORDER BY b.production_date DESC
        """)
        
        batches = []
        total_warnings = 0
        
        for row in cur.fetchall():
            # Check if this batch has all required costs
            batch_id = row[0]
            
            cur.execute("""
                SELECT COUNT(*) 
                FROM cost_elements_master 
                WHERE active = true 
                    AND applicable_to IN ('batch', 'all')
                    AND is_optional = false
            """)
            required_costs = cur.fetchone()[0]
            
            missing_costs = required_costs - row[6]
            
            if missing_costs > 0:
                total_warnings += 1
                batches.append({
                    'batch_id': row[0],
                    'batch_code': row[1],
                    'oil_type': row[2],
                    'production_date': row[3],
                    'oil_yield': float(row[4]),
                    'base_cost': float(row[5]),
                    'extended_costs': float(row[7]),
                    'missing_cost_elements': missing_costs
                })
        
        close_connection(conn, cur)
        
        return jsonify({
            'success': True,
            'total_batches_with_warnings': total_warnings,
            'batches': batches,
            'message': 'Phase 1 Validation - Warnings only'
        })
        
    except Exception as e:
        close_connection(conn, cur)
        return jsonify({'success': False, 'error': str(e)}), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
