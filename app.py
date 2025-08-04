"""
Main Flask Application for PUVI Oil Manufacturing System
Integrates all modules and provides central configuration
"""

from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime
from db_utils import get_db_connection, close_connection

# Import all module blueprints
from modules.purchase import purchase_bp
from modules.material_writeoff import writeoff_bp
from modules.batch_production import batch_bp

# Create Flask app
app = Flask(__name__)

# Enable CORS for all routes
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "http://localhost:3000",
            "https://puvi-frontend.vercel.app",
            "https://puvi-frontend-*.vercel.app"
        ],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Register all blueprints
app.register_blueprint(purchase_bp)
app.register_blueprint(writeoff_bp)
app.register_blueprint(batch_bp)

# Configuration
app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Root endpoint
@app.route('/', methods=['GET'])
def home():
    """Root endpoint to verify API is running"""
    return jsonify({
        'status': 'PUVI Backend API is running!',
        'version': '4.0',  # Modular version
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
            'inventory_items': "SELECT COUNT(*) FROM inventory WHERE closing_stock > 0"
        }
        
        counts = {}
        for key, query in queries.items():
            cur.execute(query)
            counts[key] = cur.fetchone()[0]
        
        # Get database size
        cur.execute("""
            SELECT pg_database_size(current_database()) as size
        """)
        db_size = cur.fetchone()[0]
        
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
            'version': '4.0',
            'counts': counts,
            'database_size_mb': round(db_size / 1024 / 1024, 2),
            'active_modules': sorted(active_modules),
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

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
