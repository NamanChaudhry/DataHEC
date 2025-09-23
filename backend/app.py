# app.py - Complete Flask Backend (Full Version)
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import time
import psutil
import os
import pandas as pd
import json
from datetime import datetime
import asyncio
from dotenv import load_dotenv
import header_mapping 

# Import your existing deduplication functions
try:
    from your_existing_script import (
        process_all_and_combine_final_sheets,
        generate_cross_system_winner,
        process_excel_file,
        find_fuzzy_duplicates,
        find_fuzzy_duplicates_multi_rules,
        assign_winner
    )
    print("✅ Successfully imported deduplication functions")
except ImportError as e:
    print(f"⚠️ Warning: Could not import from your_existing_script.py: {e}")
    print("Please ensure your_existing_script.py exists with the required functions")

app = Flask(__name__)
CORS(app)

# Configuration
DATA_DIR = 'data'
STATIC_DIR = 'static_data'
OUTPUT_DIR = 'outputs'
PROCESSED_OUTPUTS_DIR = 'processed_outputs'

# Ensure directories exist
for directory in [DATA_DIR, STATIC_DIR, OUTPUT_DIR, PROCESSED_OUTPUTS_DIR]:
    os.makedirs(directory, exist_ok=True)


# Registry management functions
def load_processed_outputs_registry():
    """Load the registry of processed outputs"""
    registry_file = os.path.join(PROCESSED_OUTPUTS_DIR, 'registry.json')
    if os.path.exists(registry_file):
        try:
            with open(registry_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("Warning: Invalid registry.json, creating new one")
            return {}
    return {}

def save_processed_outputs_registry(registry):
    """Save the registry of processed outputs"""
    registry_file = os.path.join(PROCESSED_OUTPUTS_DIR, 'registry.json')
    try:
        with open(registry_file, 'w') as f:
            json.dump(registry, f, indent=2)
    except Exception as e:
        print(f"Error saving registry: {e}")

def add_to_processed_outputs(entity, source_system, output_file):
    """Add a processed output to the registry"""
    registry = load_processed_outputs_registry()
    if entity not in registry:
        registry[entity] = {}
    if source_system not in registry[entity]:
        registry[entity][source_system] = []
    
    if output_file not in registry[entity][source_system]:
        registry[entity][source_system].append(output_file)
    
    save_processed_outputs_registry(registry)

# Enhanced processing functions with statistics
def process_excel_file_with_stats(file_path, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir,rules=None):
    """Enhanced version of process_excel_file that returns statistics"""
    stats_start = time.time()
    
    print(f"\n=== PROCESSING FILE WITH STATS: {file_path} ===")
    
    try:
        df = pd.read_excel(file_path)
        initial_records = len(df)
        df.columns = df.columns.str.strip()
        original_columns = df.columns.tolist()
        
        print(f"Initial records: {initial_records}")
        print(f"Columns: {len(original_columns)}")
        
        source_system = os.path.splitext(os.path.basename(file_path))[0]
        source_system_rule = source_system.split('_')[0]
        
        # Find duplicates with timing
        dup_start = time.time()
        #df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)
        if rules:  # multi-rule mode
            df = find_fuzzy_duplicates_multi_rules(df, rules)
        else:      # single-rule mode (backward compatibility)
            df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)  
        dup_time = time.time() - dup_start
        
        duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
        unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
        
        duplicate_groups = df[df.duplicated('group_id', keep=False)]['group_id'].nunique() if len(duplicate_rows) > 0 else 0
        duplicates_found = len(duplicate_rows)
        
        print(f"Duplicate detection time: {dup_time:.3f}s")
        print(f"Duplicate groups found: {duplicate_groups}")
        print(f"Duplicate records: {duplicates_found}")
        
        # Winner selection with timing
        winner_start = time.time()
        if len(duplicate_rows) > 0:
            duplicate_rows = assign_winner(duplicate_rows, source_system_rule, rulebook, is_cross_system=False)
            winner_rows = duplicate_rows[duplicate_rows['Cust_Id'] == duplicate_rows['winner']].copy()
        else:
            winner_rows = pd.DataFrame(columns=original_columns)
        
        winner_time = time.time() - winner_start
        
        final_rows = pd.concat([winner_rows[original_columns], unique_rows[original_columns]], ignore_index=True)
        final_records = len(final_rows)
        
        print(f"Winner selection time: {winner_time:.3f}s")
        print(f"Final records: {final_records}")
        
        # Save output
        output_excel_file_name = f'{source_system}_Output.xlsx'
        output_path = os.path.join(output_dir, output_excel_file_name)

        save_start = time.time()
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            final_rows.to_excel(writer, sheet_name=f'{source_system}_final'[:31], index=False)
            if len(winner_rows) > 0:
                winner_rows.to_excel(writer, sheet_name=f'{source_system}_winner'[:31], index=False)
            if len(duplicate_rows) > 0:
                duplicate_rows.to_excel(writer, sheet_name=f'{source_system}_duplicates'[:31], index=False)
            unique_rows.to_excel(writer, sheet_name=f'{source_system}_unique'[:31], index=False)
        save_time = time.time() - save_start
        
        print(f"File save time: {save_time:.3f}s")
        
        total_time = time.time() - stats_start
        print(f"Total processing time: {total_time:.3f}s")
        
        statistics = {
            'total_records': initial_records,
            'final_records': final_records,
            'duplicate_groups': duplicate_groups,
            'duplicates_found': duplicates_found,
            'unique_records': len(unique_rows),
            'duplicate_detection_time': dup_time,
            'winner_selection_time': winner_time,
            'file_save_time': save_time,
            'total_processing_time': total_time
        }
        
        return output_path, statistics
        
    except Exception as e:
        print(f"Error in process_excel_file_with_stats: {e}")
        raise

def process_output_file_with_stats(file_path, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir, source_system):
    """Enhanced version of process_output_file that returns statistics"""
    stats_start = time.time()
    
    print(f"\n=== REPROCESSING OUTPUT FILE WITH STATS: {file_path} ===")
    
    try:
        # Read from the final sheet of the output file
        try:
            df = pd.read_excel(file_path, sheet_name=f'{source_system}_final')
        except:
            df = pd.read_excel(file_path)
        
        initial_records = len(df)
        df.columns = df.columns.str.strip()
        original_columns = df.columns.tolist()
        
        print(f"Initial records: {initial_records}")
        
        # Find duplicates with timing
        dup_start = time.time()
        df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)
        dup_time = time.time() - dup_start
        
        duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
        unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
        
        duplicate_groups = df[df.duplicated('group_id', keep=False)]['group_id'].nunique() if len(duplicate_rows) > 0 else 0
        duplicates_found = len(duplicate_rows)
        
        print(f"Duplicate detection time: {dup_time:.3f}s")
        print(f"Duplicate groups found: {duplicate_groups}")
        
        # Winner selection with timing
        winner_start = time.time()
        if len(duplicate_rows) > 0:
            duplicate_rows = assign_winner(duplicate_rows, source_system, rulebook, is_cross_system=False)
            winner_rows = duplicate_rows[duplicate_rows['Cust_Id'] == duplicate_rows['winner']].copy()
        else:
            winner_rows = pd.DataFrame(columns=original_columns)
        
        winner_time = time.time() - winner_start
        
        final_rows = pd.concat([winner_rows[original_columns], unique_rows[original_columns]], ignore_index=True)
        final_records = len(final_rows)
        
        print(f"Winner selection time: {winner_time:.3f}s")
        
        # Generate new output filename to avoid overwriting
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_excel_file_name = f'{base_name}_Reprocessed_{timestamp}.xlsx'
        output_path = os.path.join(output_dir, output_excel_file_name)

        save_start = time.time()
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            final_rows.to_excel(writer, sheet_name=f'{source_system}_final'[:31], index=False)
            if len(winner_rows) > 0:
                winner_rows.to_excel(writer, sheet_name=f'{source_system}_winner'[:31], index=False)
            if len(duplicate_rows) > 0:
                duplicate_rows.to_excel(writer, sheet_name=f'{source_system}_duplicates'[:31], index=False)
            unique_rows.to_excel(writer, sheet_name=f'{source_system}_unique'[:31], index=False)
        save_time = time.time() - save_start
        
        total_time = time.time() - stats_start
        
        statistics = {
            'total_records': initial_records,
            'final_records': final_records,
            'duplicate_groups': duplicate_groups,
            'duplicates_found': duplicates_found,
            'unique_records': len(unique_rows),
            'duplicate_detection_time': dup_time,
            'winner_selection_time': winner_time,
            'file_save_time': save_time,
            'total_processing_time': total_time
        }
        
        return output_path, statistics
        
    except Exception as e:
        print(f"Error in process_output_file_with_stats: {e}")
        raise

@app.route('/api/header-mapping', methods=['GET'])
def get_header_mapping():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        mapping = loop.run_until_complete(header_mapping.main())
        loop.close()

        if mapping is None:
            return jsonify({"error": "No mapping returned"}), 500

        return jsonify(mapping)   # ✅ Now sends JSON to frontend
    except Exception as e:
        print(f"Error in /api/header-mapping: {e}")
        return jsonify({"error": str(e)}), 500

# API Routes
# --- MATCH RULES API ---
MATCH_RULES_FILE = os.path.join(STATIC_DIR, "matchrules.json")

def load_match_rules():
    if os.path.exists(MATCH_RULES_FILE):
        with open(MATCH_RULES_FILE, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

def save_match_rules(rules):
    with open(MATCH_RULES_FILE, "w") as f:
        json.dump(rules, f, indent=2)

@app.route("/api/match-rules", methods=["GET"])
def get_match_rules():
    """Fetch all match rules"""
    try:
        rules = load_match_rules()
        return jsonify(rules)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/api/match-rules", methods=["POST"])
def add_match_rule():
    """Add a new match rule"""
    try:
        data = request.json
        new_rule_name = data.get("rule")
        new_description = data.get("description", "")
        fuzzy_columns = data.get("fuzzy_columns", [])
        exact_columns = data.get("exact_columns", [])
        thresholds = data.get("thresholds", {})

        if not new_rule_name:
            return jsonify({"error": "Rule name is required"}), 400

        rules = load_match_rules()

        # Check if rule name already exists (case insensitive)
        if any(r["rule"].strip().lower() == new_rule_name.strip().lower() for r in rules):
            return jsonify({"error": "Rule already exists"}), 400

        # Append new rule object
        rules.append({
            "rule": new_rule_name.strip(),
            "description": new_description.strip(),
            "fuzzy_columns": fuzzy_columns,
            "exact_columns": exact_columns,
            "thresholds": thresholds
        })

        save_match_rules(rules)

        return jsonify({"message": "Rule added successfully", "rules": rules})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/match-rules", methods=["PUT"])
def update_match_rule():
    """Update an existing match rule"""
    try:
        data = request.json
        old_rule = data.get("oldRule")
        new_rule = data.get("rule")
        description = data.get("description", "")
        fuzzy_columns = data.get("fuzzy_columns", [])
        exact_columns = data.get("exact_columns", [])
        thresholds = data.get("thresholds", {})

        if not old_rule or not new_rule:
            return jsonify({"error": "oldRule and rule are required"}), 400

        rules = load_match_rules()

        # Find index of the rule by matching rule name (case insensitive & trimmed)
        index = next(
            (i for i, r in enumerate(rules)
             if r["rule"].strip().lower() == old_rule.strip().lower()),
            None
        )

        if index is None:
            return jsonify({"error": "Rule not found"}), 404

        # Check for name conflict with other rules
        if any(r["rule"].strip().lower() == new_rule.strip().lower() and i != index
               for i, r in enumerate(rules)):
            return jsonify({"error": "Rule already exists"}), 400

        # Helper to clean column lists
        def clean_columns(cols):
            if not isinstance(cols, list):
                return []
            return [col.strip() for col in cols if isinstance(col, str) and col.strip()]

        # Validate thresholds is dict
        if not isinstance(thresholds, dict):
            thresholds = {}

        # Update the rule
        rules[index] = {
            "rule": new_rule.strip(),
            "description": description.strip(),
            "fuzzy_columns": clean_columns(fuzzy_columns),
            "exact_columns": clean_columns(exact_columns),
            "thresholds": thresholds
        }

        save_match_rules(rules)

        return jsonify({"message": "Rule updated successfully", "rules": rules})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/api/match-rules", methods=["DELETE"])
def delete_match_rule():
    """Delete a match rule"""
    try:
        data = request.json
        rule_to_delete = data.get("rule")

        if not rule_to_delete:
            return jsonify({"error": "Rule name is required"}), 400

        rules = load_match_rules()

        # Find index of the rule by matching rule name (case insensitive & trimmed)
        index = next(
            (i for i, r in enumerate(rules)
                if r["rule"].strip().lower() == rule_to_delete.strip().lower()),
            None
        )

        if index is None:
            return jsonify({"error": "Rule not found"}), 404

        # Remove the rule
        rules.pop(index)

        save_match_rules(rules)

        return jsonify({"message": "Rule deleted successfully", "rules": rules})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/entities', methods=['GET'])
def get_entities():
    """Get all available entities"""
    try:
        if not os.path.exists(DATA_DIR):
            return jsonify([])
        entities = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
        return jsonify(sorted(entities))
    except Exception as e:
        print(f"Error in get_entities: {e}")
        return jsonify({"error": str(e)}), 500
@app.route('/api/source-systems/<entity>', methods=['GET'])
def get_source_systems(entity):
    """Get all source systems for an entity"""
    try:
        path = os.path.join(DATA_DIR, entity)
        if not os.path.exists(path):
            return jsonify({"error": f"Entity {entity} not found"}), 404
        source_systems = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
        return jsonify(sorted(source_systems))
    except Exception as e:
        print(f"Error in get_source_systems: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/files/<entity>/<source_system>', methods=['GET'])
def get_files(entity, source_system):
    """Get all files for an entity and source system"""
    try:
        path = os.path.join(DATA_DIR, entity, source_system)
        if not os.path.exists(path):
            return jsonify({"error": f"Path {entity}/{source_system} not found"}), 404
        files = [f for f in os.listdir(path) if f.endswith(('.xlsx', '.xls'))]
        return jsonify(sorted(files))
    except Exception as e:
        print(f"Error in get_files: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/processed-outputs/<entity>', methods=['GET'])
def get_processed_outputs(entity):
    """Get all processed outputs for an entity"""
    try:
        registry = load_processed_outputs_registry()
        return jsonify(registry.get(entity, {}))
    except Exception as e:
        print(f"Error in get_processed_outputs: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/columns/<entity>/<source_system>/<filename>', methods=['GET'])
def get_columns_for_file(entity, source_system, filename):
    """Get columns from a source file"""
    try:
        path = os.path.join(DATA_DIR, entity, source_system, filename)
        if not os.path.exists(path):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        df = pd.read_excel(path, nrows=0)  # Read only header
        columns = [col.strip() for col in df.columns.tolist()]
        return jsonify(columns)
    except Exception as e:
        print(f"Error in get_columns_for_file: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/output-columns/<filename>', methods=['GET'])
def get_output_columns(filename):
    """Get columns from a processed output file"""
    try:
        path = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(path):
            return jsonify({"error": f"Output file {filename} not found"}), 404
        
        # Try to read from the final sheet first
        try:
            # Extract source system from filename (e.g., "ps91_Output.xlsx" -> "ps91")
            source_system = filename.replace('_Output.xlsx', '').split('_')[0]
            df = pd.read_excel(path, sheet_name=f'{source_system}_final', nrows=0)
        except:
            # Fallback to first sheet
            df = pd.read_excel(path, nrows=0)
        
        columns = [col.strip() for col in df.columns.tolist()]
        return jsonify(columns)
    except Exception as e:
        print(f"Error in get_output_columns: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/process-single', methods=['POST'])
def process_single_file():
    """Process a single file with detailed timing and statistics"""
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    try:
        print(f"\n=== SINGLE FILE PROCESSING START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        data = request.json
        
        # Extract parameters
        entity = data.get('entity')
        source_system = data.get('source_system')
        filename = data.get('filename')
        file_type = data.get('file_type', 'source')
        fuzzy_columns = data.get('fuzzy_columns', [])
        exact_columns = data.get('exact_columns', [])
        thresholds = data.get('thresholds', {})
        rules = data.get('rules', [])

        # Validation
        if not all([entity, source_system, filename]):
            return jsonify({"error": "Missing required parameters: entity, source_system, filename"}), 400

        print(f"Entity: {entity}")
        print(f"Source System: {source_system}")
        print(f"Filename: {filename}")
        print(f"File Type: {file_type}")
        print(f"Fuzzy Columns: {fuzzy_columns}")
        print(f"Exact Columns: {exact_columns}")
        print(f"Thresholds: {thresholds}")

        # File loading phase
        file_load_start = time.time()
        
        # Determine file path based on type
        if file_type == 'source':
            filepath = os.path.join(DATA_DIR, entity, source_system, filename)
        else:  # output
            filepath = os.path.join(OUTPUT_DIR, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File not found: {filepath}"}), 404

        # Get file size
        file_size_mb = os.path.getsize(filepath) / 1024 / 1024
        print(f"File size: {file_size_mb:.2f} MB")

        # Load rulebook
        rulebook_path = os.path.join(STATIC_DIR, 'Rulebook.xlsx')
        if not os.path.exists(rulebook_path):
            return jsonify({"error": "Rulebook.xlsx not found in static_data directory"}), 404

        rulebook = pd.read_excel(rulebook_path)
        file_load_time = time.time() - file_load_start
        print(f"File loading time: {file_load_time:.3f} seconds")

        # Processing phase
        processing_start = time.time()
        
        # Process based on file type
        if file_type == 'output':
            output_file, processing_stats = process_output_file_with_stats(
                filepath, fuzzy_columns, exact_columns, thresholds, rulebook, OUTPUT_DIR, source_system
            )
        else:
            output_file, processing_stats = process_excel_file_with_stats(
                filepath, fuzzy_columns, exact_columns, thresholds, rulebook, OUTPUT_DIR,rules=rules
            )

        processing_time = time.time() - processing_start
        print(f"Processing time: {processing_time:.3f} seconds")

        # Add to processed outputs registry
        output_filename = os.path.basename(output_file)
        add_to_processed_outputs(entity, source_system, output_filename)

        # Calculate final statistics
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        total_time = end_time - start_time
        memory_used = max(0, end_memory - start_memory)

        print(f"=== PROCESSING COMPLETE ===")
        print(f"Total time: {total_time:.3f} seconds")
        print(f"Memory used: {memory_used:.2f} MB")
        if processing_stats.get('total_records', 0) > 0:
            print(f"Records per second: {processing_stats.get('total_records', 0) / total_time:.0f}")

        return jsonify({
            "message": f"✅ Processing complete! Output file: {output_filename}",
            "output_file": output_filename,
            "download_link": f"/api/download/{output_filename}",
            "processing_time_ms": int(total_time * 1000),
            "file_load_time_ms": int(file_load_time * 1000),
            "processing_only_time_ms": int(processing_time * 1000),
            "memory_used_mb": round(memory_used, 2),
            "file_size_mb": round(file_size_mb, 2),
            "total_records": processing_stats.get('total_records', 0),
            "duplicate_groups": processing_stats.get('duplicate_groups', 0),
            "final_records": processing_stats.get('final_records', 0),
            "duplicates_found": processing_stats.get('duplicates_found', 0),
            "performance_stats": {
                "records_per_second": round(processing_stats.get('total_records', 0) / max(total_time, 0.001), 0),
                "mb_per_second": round(file_size_mb / max(total_time, 0.001), 2),
                "fuzzy_columns_count": len(fuzzy_columns),
                "exact_columns_count": len(exact_columns)
            }
        })

    except Exception as e:
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Error in process_single_file after {total_time:.3f}s: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "processing_time_ms": int(total_time * 1000),
            "failed": True
        }), 500

@app.route('/api/process-cross-system', methods=['POST'])
def process_cross_system():
    """Process multiple files for cross-system deduplication with detailed timing"""
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    
    try:
        print(f"\n=== CROSS SYSTEM PROCESSING START: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        data = request.json
        
        entity = data.get('entity')
        file_configs = data.get('file_configs', [])
        global_fuzzy_columns = data.get('global_fuzzy_columns', [])
        global_exact_columns = data.get('global_exact_columns', [])
        global_thresholds = data.get('global_thresholds', {})

        # Validation
        if not entity:
            return jsonify({"error": "Missing required parameter: entity"}), 400
        
        if not file_configs:
            return jsonify({"error": "No file configurations provided"}), 400

        print(f"Processing {len(file_configs)} file configurations for entity: {entity}")

        # Load required files
        rulebook_path = os.path.join(STATIC_DIR, 'Rulebook.xlsx')
        source_system_mapping_path = os.path.join(STATIC_DIR, 'Source_System_Mapping.xlsx')
        
        if not os.path.exists(rulebook_path):
            return jsonify({"error": "Rulebook.xlsx not found in static_data directory"}), 404
        
        if not os.path.exists(source_system_mapping_path):
            return jsonify({"error": "Source_System_Mapping.xlsx not found in static_data directory"}), 404

        rulebook = pd.read_excel(rulebook_path)
        source_system_main_file = pd.read_excel(source_system_mapping_path)

        # File reading phase
        file_read_start = time.time()
        all_dataframes = []
        total_input_records = 0
        file_sizes = []
        
        for i, config in enumerate(file_configs):
            print(f"Reading file {i+1}/{len(file_configs)}: {config.get('source_system', 'unknown')}/{config.get('filename', 'unknown')}")
            
            # Determine file path based on type
            if config.get('file_type') == 'output':
                path = os.path.join(OUTPUT_DIR, config['filename'])
            else:
                path = os.path.join(DATA_DIR, entity, config['source_system'], config['filename'])
                
            if not os.path.exists(path):
                return jsonify({"error": f"File not found: {path}"}), 404
            
            # Get file size
            file_size_mb = os.path.getsize(path) / 1024 / 1024
            file_sizes.append(file_size_mb)
            print(f"File size: {file_size_mb:.2f} MB")
            
            try:
                if config.get('file_type') == 'output':
                    # Read from the final sheet of processed output
                    try:
                        source_system = config['source_system']
                        df = pd.read_excel(path, sheet_name=f"{source_system}_final")
                    except:
                        df = pd.read_excel(path)
                else:
                    # Read source file directly
                    df = pd.read_excel(path)
                
                df['Source_System'] = config['source_system']
                all_dataframes.append(df)
                total_input_records += len(df)
                print(f"Loaded {len(df)} rows from {config['source_system']}/{config['filename']}")
                
            except Exception as e:
                print(f"Error reading file {path}: {e}")
                return jsonify({"error": f"Error reading file {config['filename']}: {str(e)}"}), 500

        file_read_time = time.time() - file_read_start
        print(f"File reading completed in {file_read_time:.3f}s")
        print(f"Total input records: {total_input_records}")
        print(f"Total file size: {sum(file_sizes):.2f} MB")

        if not all_dataframes:
            return jsonify({"error": "No valid data found in selected files"}), 400

        # Combine dataframes phase
        combine_start = time.time()
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combine_time = time.time() - combine_start
        print(f"Dataframe combination time: {combine_time:.3f}s")
        print(f"Combined dataframe shape: {combined_df.shape}")

        # Save combined file
        save_start = time.time()
        combined_excel_path = os.path.join(OUTPUT_DIR, f'{entity}_CrossSystem_Combined.xlsx')
        with pd.ExcelWriter(combined_excel_path, engine='openpyxl') as writer:
            combined_df.to_excel(writer, sheet_name='crosssystem_input', index=False)
        save_time = time.time() - save_start
        print(f"Combined file save time: {save_time:.3f}s")

        # Cross-system deduplication phase
        dedup_start = time.time()
        final_cross_output = generate_cross_system_winner(
            combined_excel_path,
            rulebook,
            global_fuzzy_columns,
            global_exact_columns,
            global_thresholds,
            source_system_main_file,
            OUTPUT_DIR
        )
        dedup_time = time.time() - dedup_start
        print(f"Cross-system deduplication time: {dedup_time:.3f}s")

        # Read final results to get statistics
        try:
            final_df = pd.read_excel(final_cross_output, sheet_name='crosssystem_final')
            final_records = len(final_df)
            
            # Try to get duplicate statistics
            try:
                dup_df = pd.read_excel(final_cross_output, sheet_name='all_duplicates')
                duplicate_groups = dup_df['group_id'].nunique() if 'group_id' in dup_df.columns else 0
                duplicates_found = len(dup_df)
            except:
                duplicate_groups = 0
                duplicates_found = 0
                
        except Exception as e:
            print(f"Error reading final results: {e}")
            final_records = 0
            duplicate_groups = 0
            duplicates_found = 0

        # Prepare output files list
        output_files = [
            os.path.basename(combined_excel_path),
            os.path.basename(final_cross_output)
        ]

        # Calculate final statistics
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        total_time = end_time - start_time
        memory_used = max(0, end_memory - start_memory)

        print(f"=== CROSS-SYSTEM PROCESSING COMPLETE ===")
        print(f"Total time: {total_time:.3f} seconds")
        print(f"Memory used: {memory_used:.2f} MB")
        print(f"Records per second: {total_input_records / max(total_time, 0.001):.0f}")
        print(f"Final output files: {output_files}")

        message = f"✅ Cross-system processing complete! Processed {len(file_configs)} files with global column settings."

        return jsonify({
            "message": message,
            "outputs": output_files,
            "download_links": [f"/api/download/{filename}" for filename in output_files],
            "processing_time_ms": int(total_time * 1000),
            "file_read_time_ms": int(file_read_time * 1000),
            "combine_time_ms": int(combine_time * 1000),
            "deduplication_time_ms": int(dedup_time * 1000),
            "save_time_ms": int(save_time * 1000),
            "memory_used_mb": round(memory_used, 2),
            "total_file_size_mb": round(sum(file_sizes), 2),
            "total_records": total_input_records,
            "final_records": final_records,
            "duplicate_groups": duplicate_groups,
            "duplicates_found": duplicates_found,
            "performance_stats": {
                "records_per_second": round(total_input_records / max(total_time, 0.001), 0),
                "mb_per_second": round(sum(file_sizes) / max(total_time, 0.001), 2),
                "files_processed": len(file_configs),
                "fuzzy_columns_count": len(global_fuzzy_columns),
                "exact_columns_count": len(global_exact_columns)
            }
        })

    except Exception as e:
        end_time = time.time()
        total_time = end_time - start_time
        print(f"ERROR in process_cross_system after {total_time:.3f}s: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "processing_time_ms": int(total_time * 1000),
            "failed": True
        }), 500

@app.route('/api/download/<filename>', methods=['GET'])
def download_output(filename):
    """Download a processed output file"""
    try:
        file_path = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
        return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)
    except Exception as e:
        print(f"Error in download_output: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/clear-processed-outputs/<entity>', methods=['DELETE'])
def clear_processed_outputs(entity):
    """Clear all processed outputs for an entity - both registry and actual files"""
    try:
        registry = load_processed_outputs_registry()
        deleted_files = []
        
        if entity in registry:
            # Get all output files for this entity
            for source_system, outputs in registry[entity].items():
                for output_file in outputs:
                    file_path = os.path.join(OUTPUT_DIR, output_file)
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            deleted_files.append(output_file)
                            print(f"Deleted file: {output_file}")
                        except Exception as e:
                            print(f"Error deleting file {output_file}: {e}")
            
            # Remove from registry
            del registry[entity]
            save_processed_outputs_registry(registry)
        
        return jsonify({
            "message": f"✅ Cleared {len(deleted_files)} processed output files for {entity}",
            "deleted_files": deleted_files
        })
    except Exception as e:
        print(f"Error in clear_processed_outputs: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/clear-specific-output/<entity>/<source_system>/<filename>', methods=['DELETE'])
def clear_specific_output(entity, source_system, filename):
    """Clear a specific processed output file"""
    try:
        registry = load_processed_outputs_registry()
        
        # Remove file
        file_path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted file: {filename}")
        
        # Remove from registry
        if entity in registry and source_system in registry[entity]:
            if filename in registry[entity][source_system]:
                registry[entity][source_system].remove(filename)
                
                # Clean up empty entries
                if not registry[entity][source_system]:
                    del registry[entity][source_system]
                if not registry[entity]:
                    del registry[entity]
                
                save_processed_outputs_registry(registry)
        
        return jsonify({
            "message": f"✅ Deleted {filename}",
            "deleted_file": filename
        })
    except Exception as e:
        print(f"Error in clear_specific_output: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Comprehensive system health check endpoint"""
    try:
        # Check directories
        dirs_status = {
            'data_dir': {
                'path': os.path.abspath(DATA_DIR),
                'exists': os.path.exists(DATA_DIR),
                'readable': os.access(DATA_DIR, os.R_OK) if os.path.exists(DATA_DIR) else False
            },
            'static_dir': {
                'path': os.path.abspath(STATIC_DIR),
                'exists': os.path.exists(STATIC_DIR),
                'readable': os.access(STATIC_DIR, os.R_OK) if os.path.exists(STATIC_DIR) else False
            },
            'output_dir': {
                'path': os.path.abspath(OUTPUT_DIR),
                'exists': os.path.exists(OUTPUT_DIR),
                'writable': os.access(OUTPUT_DIR, os.W_OK) if os.path.exists(OUTPUT_DIR) else False
            },
            'processed_outputs_dir': {
                'path': os.path.abspath(PROCESSED_OUTPUTS_DIR),
                'exists': os.path.exists(PROCESSED_OUTPUTS_DIR),
                'writable': os.access(PROCESSED_OUTPUTS_DIR, os.W_OK) if os.path.exists(PROCESSED_OUTPUTS_DIR) else False
            }
        }
        
        # Check required files
        required_files = {
            'rulebook': {
                'path': os.path.join(STATIC_DIR, 'Rulebook.xlsx'),
                'exists': os.path.exists(os.path.join(STATIC_DIR, 'Rulebook.xlsx'))
            },
            'source_mapping': {
                'path': os.path.join(STATIC_DIR, 'Source_System_Mapping.xlsx'),
                'exists': os.path.exists(os.path.join(STATIC_DIR, 'Source_System_Mapping.xlsx'))
            },
            'registry': {
                'path': os.path.join(PROCESSED_OUTPUTS_DIR, 'registry.json'),
                'exists': os.path.exists(os.path.join(PROCESSED_OUTPUTS_DIR, 'registry.json'))
            }
        }
        
        # Get system info
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('.')
        
        system_info = {
            'memory': {
                'total_gb': round(memory.total / 1024 / 1024 / 1024, 2),
                'available_gb': round(memory.available / 1024 / 1024 / 1024, 2),
                'used_percent': memory.percent
            },
            'disk': {
                'total_gb': round(disk.total / 1024 / 1024 / 1024, 2),
                'free_gb': round(disk.free / 1024 / 1024 / 1024, 2),
                'used_percent': round((disk.used / disk.total) * 100, 2)
            },
            'cpu_percent': psutil.cpu_percent(interval=1)
        }
        
        # Count entities and files
        entities_count = 0
        total_files = 0
        total_outputs = 0
        
        if os.path.exists(DATA_DIR):
            entities = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
            entities_count = len(entities)
            
            for entity in entities:
                entity_path = os.path.join(DATA_DIR, entity)
                if os.path.isdir(entity_path):
                    for item in os.listdir(entity_path):
                        source_path = os.path.join(entity_path, item)
                        if os.path.isdir(source_path):
                            files = [f for f in os.listdir(source_path) if f.endswith(('.xlsx', '.xls'))]
                            total_files += len(files)
        
        if os.path.exists(OUTPUT_DIR):
            output_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(('.xlsx', '.xls'))]
            total_outputs = len(output_files)
        
        stats = {
            'entities_count': entities_count,
            'total_source_files': total_files,
            'total_output_files': total_outputs
        }
        
        # Determine overall health
        all_dirs_ok = all(d['exists'] for d in dirs_status.values())
        required_files_ok = all(f['exists'] for f in required_files.values())
        memory_ok = memory.percent < 90
        disk_ok = (disk.used / disk.total) < 0.9
        
        overall_status = "healthy" if all([all_dirs_ok, required_files_ok, memory_ok, disk_ok]) else "warning"
        
        return jsonify({
            "status": overall_status,
            "timestamp": datetime.now().isoformat(),
            "directories": dirs_status,
            "required_files": required_files,
            "system_info": system_info,
            "statistics": stats,
            "checks": {
                "directories_ok": all_dirs_ok,
                "required_files_ok": required_files_ok,
                "memory_ok": memory_ok,
                "disk_ok": disk_ok
            }
        })
        
    except Exception as e:
        print(f"Error in health_check: {e}")
        return jsonify({
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }), 500

@app.route('/api/system-info', methods=['GET'])
def get_system_info():
    """Get detailed system information for performance monitoring"""
    try:
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        disk = psutil.disk_usage('.')
        
        # Get process info
        process = psutil.Process()
        process_memory = process.memory_info()
        
        return jsonify({
            "system": {
                "cpu_percent": cpu_percent,
                "memory": {
                    "total_gb": round(memory.total / 1024 / 1024 / 1024, 2),
                    "available_gb": round(memory.available / 1024 / 1024 / 1024, 2),
                    "used_gb": round((memory.total - memory.available) / 1024 / 1024 / 1024, 2),
                    "used_percent": memory.percent
                },
                "disk": {
                    "total_gb": round(disk.total / 1024 / 1024 / 1024, 2),
                    "free_gb": round(disk.free / 1024 / 1024 / 1024, 2),
                    "used_gb": round(disk.used / 1024 / 1024 / 1024, 2),
                    "used_percent": round((disk.used / disk.total) * 100, 2)
                }
            },
            "process": {
                "memory_mb": round(process_memory.rss / 1024 / 1024, 2),
                "cpu_percent": process.cpu_percent(),
                "pid": process.pid,
                "status": process.status()
            },
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Error in get_system_info: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/performance-benchmark', methods=['POST'])
def performance_benchmark():
    """Run a performance benchmark test"""
    try:
        data = request.json
        test_size = data.get('test_size', 1000)  # Number of test records
        
        print(f"Running performance benchmark with {test_size} test records...")
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Create test data
        import numpy as np
        import random
        import string
        
        def random_string(length):
            return ''.join(random.choices(string.ascii_lowercase, k=length))
        
        test_data = {
            'Cust_Id': list(range(1, test_size + 1)),
            'first_name': [random_string(8) for _ in range(test_size)],
            'last_name': [random_string(10) for _ in range(test_size)],
            'email': [f'{random_string(5)}@{random_string(5)}.com' for _ in range(test_size)],
            'phone': [f'555-{random.randint(1000, 9999)}' for _ in range(test_size)],
            'Transaction Date': pd.date_range('2023-01-01', periods=test_size, freq='D')
        }
        
        # Add some duplicates for testing
        duplicate_percent = 0.1  # 10% duplicates
        num_duplicates = int(test_size * duplicate_percent)
        
        for i in range(num_duplicates):
            idx = random.randint(1, test_size - 1)
            test_data['first_name'][idx] = test_data['first_name'][0]  # Make it similar to first record
            test_data['last_name'][idx] = test_data['last_name'][0]
        
        df = pd.DataFrame(test_data)
        
        # Test fuzzy matching performance
        fuzzy_columns = ['first_name', 'last_name']
        exact_columns = ['email']
        thresholds = {'first_name': 90, 'last_name': 90}
        
        processing_start = time.time()
        
        # Use the actual deduplication function
        df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, thresholds)
        
        processing_time = time.time() - processing_start
        total_time = time.time() - start_time
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_used = max(0, end_memory - start_memory)
        
        # Calculate statistics
        duplicate_rows = df[df.duplicated('group_id', keep=False)]
        duplicate_groups = df[df.duplicated('group_id', keep=False)]['group_id'].nunique() if len(duplicate_rows) > 0 else 0
        
        benchmark_results = {
            "test_parameters": {
                "test_size": test_size,
                "fuzzy_columns": fuzzy_columns,
                "exact_columns": exact_columns,
                "duplicate_percent": duplicate_percent
            },
            "performance": {
                "total_time_ms": round(total_time * 1000, 2),
                "processing_time_ms": round(processing_time * 1000, 2),
                "memory_used_mb": round(memory_used, 2),
                "records_per_second": round(test_size / max(processing_time, 0.001), 0),
                "memory_per_record_kb": round((memory_used * 1024) / max(test_size, 1), 2)
            },
            "results": {
                "total_records": test_size,
                "duplicate_groups": duplicate_groups,
                "duplicate_records": len(duplicate_rows),
                "unique_records": test_size - len(duplicate_rows)
            },
            "timestamp": datetime.now().isoformat()
        }
        
        print(f"Benchmark completed: {test_size} records in {processing_time:.3f}s")
        print(f"Performance: {test_size / max(processing_time, 0.001):.0f} records/second")
        
        return jsonify(benchmark_results)
        
    except Exception as e:
        print(f"Error in performance_benchmark: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/file-info/<entity>/<source_system>/<filename>', methods=['GET'])
def get_file_info(entity, source_system, filename):
    """Get detailed information about a specific file"""
    try:
        path = os.path.join(DATA_DIR, entity, source_system, filename)
        if not os.path.exists(path):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        # Get file statistics
        file_stats = os.stat(path)
        file_size_mb = file_stats.st_size / 1024 / 1024
        
        # Read file to get record count and columns
        df = pd.read_excel(path, nrows=1000)  # Sample first 1000 rows for performance
        
        # Get full record count
        total_records = len(pd.read_excel(path, usecols=[0]))  # Read only first column for count
        
        columns_info = []
        for col in df.columns:
            col_data = df[col].dropna()
            col_info = {
                'name': col.strip(),
                'dtype': str(df[col].dtype),
                'non_null_count': len(col_data),
                'sample_values': col_data.head(5).tolist() if len(col_data) > 0 else []
            }
            columns_info.append(col_info)
        
        return jsonify({
            "filename": filename,
            "file_size_mb": round(file_size_mb, 2),
            "total_records": total_records,
            "columns_count": len(df.columns),
            "columns_info": columns_info,
            "last_modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
            "created": datetime.fromtimestamp(file_stats.st_ctime).isoformat()
        })
        
    except Exception as e:
        print(f"Error in get_file_info: {e}")
        return jsonify({"error": str(e)}), 500
    

# import pandas as pd
# import os
# import random
# from fuzzywuzzy import fuzz
# from itertools import combinations


# def find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90):
#     print(f"Finding duplicates with fuzzy_columns: {fuzzy_columns}, exact_columns: {exact_columns}")
    
#     df['group_id'] = None
#     df['match_percentage'] = 0.0
#     for column in fuzzy_columns:
#         df[f'{column}_fuzzy_match_percentage'] = 0.0

#     group_id = 1
#     groups = {}

#     for a, b in combinations(df.index, 2):
#         match_scores = {}
#         for column in fuzzy_columns:
#             threshold = fuzzy_thresholds.get(column, 90)
#             match_score = fuzz.ratio(str(df.at[a, column]), str(df.at[b, column]))
#             match_scores[column] = match_score
#             if match_score < threshold:
#                 break

#         if all(match_scores[column] >= fuzzy_thresholds.get(column, 90) for column in fuzzy_columns):
#             exact_match = all(df.at[a, col] == df.at[b, col] for col in exact_columns)
#             overall_match_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0

#             if exact_match and overall_match_score >= exact_threshold:
#                 group_a = groups.get(a)
#                 group_b = groups.get(b)
#                 if group_a and group_b and group_a != group_b:
#                     for record, group in groups.items():
#                         if group == group_b:
#                             groups[record] = group_a
#                 elif group_a or group_b:
#                     assigned_group = group_a or group_b
#                     groups[a] = assigned_group
#                     groups[b] = assigned_group
#                 else:
#                     groups[a] = group_id
#                     groups[b] = group_id
#                     group_id += 1

#                 for column in fuzzy_columns:
#                     df.at[a, f'{column}_fuzzy_match_percentage'] = match_scores[column]
#                     df.at[b, f'{column}_fuzzy_match_percentage'] = match_scores[column]

#                 df.at[a, 'match_percentage'] = float(overall_match_score)
#                 df.at[b, 'match_percentage'] = float(overall_match_score)

#     for index, group in groups.items():
#         df.at[index, 'group_id'] = group

#     for index, row in df.iterrows():
#         if pd.isnull(row['group_id']):
#             df.at[index, 'group_id'] = group_id
#             group_id += 1

#     print(f"Found {len([g for g in df['group_id'].value_counts() if g > 1])} duplicate groups")
#     return df


# def assign_winner(df, source_system, rulebook, is_cross_system=False, source_system_main_file=None):
#     print(f"Assigning winners for source_system: {source_system}, is_cross_system: {is_cross_system}")
    
#     df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
#     df['winner'] = None

#     if not is_cross_system:
#         criteria_row = rulebook[rulebook['source_system'] == source_system]
#         if criteria_row.empty:
#             print(f"WARNING: Source system {source_system} not found in rulebook. Using default criteria.")
#             winning_criteria = 'latest_transaction_date'
#         else:
#             winning_criteria = criteria_row['winning_criteria'].values[0]

#         print(f"Using winning criteria: {winning_criteria}")

#         for group_id, group in df.groupby('group_id'):
#             try:
#                 if winning_criteria == 'latest_transaction_date':
#                     winner_id = group.sort_values(by='Transaction Date', ascending=False).iloc[0]['Cust_Id']
#                 elif winning_criteria == 'earliest_transaction_date':
#                     winner_id = group.sort_values(by='Transaction Date', ascending=True).iloc[0]['Cust_Id']
#                 elif winning_criteria == 'largest_name':
#                     winner_id = group.loc[group['first_name'].str.len().idxmax()]['Cust_Id']
#                 else:
#                     winner_id = group.sort_values(by='Transaction Date', ascending=False).iloc[0]['Cust_Id']
#                 df.loc[df['group_id'] == group_id, 'winner'] = winner_id
#             except Exception as e:
#                 print(f"Error selecting winner for group {group_id} in {source_system}: {e}")
#     else:
#         print("Processing cross-system winner selection...")
#         df['winner_source'] = None
#         for group_id, group in df.groupby('group_id'):
#             try:
#                 group_priorities = group[['Cust_Id', 'Source_System']].rename(columns={'Source_System': 'source_system'}) \
#                     .merge(source_system_main_file, on='source_system', how='left')

#                 group_priorities = group_priorities.sort_values(by='precedence')
#                 winner_id = group_priorities.iloc[0]['Cust_Id']
#                 winner_source = group_priorities.iloc[0]['source_system']

#                 df.loc[df['group_id'] == group_id, 'winner'] = winner_id
#                 df.loc[df['group_id'] == group_id, 'winner_source'] = winner_source
#             except Exception as e:
#                 print(f"Error selecting cross-system winner for group {group_id}: {e}")

#     return df


# def process_excel_file(file_path, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
#     print(f"\n=== PROCESSING FILE: {file_path} ===")
#     print(f"Fuzzy columns: {fuzzy_columns}")
#     print(f"Exact columns: {exact_columns}")
#     print(f"Thresholds: {fuzzy_thresholds}")
    
#     df = pd.read_excel(file_path)
#     df.columns = df.columns.str.strip()
#     original_columns = df.columns.tolist()
#     print(f"Original columns: {original_columns}")
#     print(f"Data shape: {df.shape}")

#     source_system = os.path.splitext(os.path.basename(file_path))[0]
#     source_system_rule = source_system.split('_')[0]
#     print(f"Source system: {source_system}, Rule system: {source_system_rule}")

#     df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)

#     duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
#     unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
    
#     print(f"Duplicate rows: {len(duplicate_rows)}, Unique rows: {len(unique_rows)}")

#     duplicate_rows = assign_winner(duplicate_rows, source_system_rule, rulebook, is_cross_system=False)
#     winner_rows = duplicate_rows[duplicate_rows['Cust_Id'] == duplicate_rows['winner']].copy()

#     final_rows = pd.concat([winner_rows[original_columns], unique_rows[original_columns]], ignore_index=True)
#     print(f"Final rows count: {len(final_rows)}")

#     output_excel_file_name = f'{source_system}_Output.xlsx'
#     output_path = os.path.join(output_dir, output_excel_file_name)

#     with pd.ExcelWriter(output_path) as writer:
#         final_rows.to_excel(writer, sheet_name=f'{source_system}_final'[:31], index=False)
#         winner_rows.to_excel(writer, sheet_name=f'{source_system}_winner'[:31], index=False)
#         duplicate_rows.to_excel(writer, sheet_name=f'{source_system}_duplicates'[:31], index=False)
#         unique_rows.to_excel(writer, sheet_name=f'{source_system}_unique'[:31], index=False)

#     print(f"Output saved to: {output_path}")
#     return output_path


# def process_all_and_combine_final_sheets(file_list, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
#     print(f"\n=== COMBINING FINAL SHEETS ===")
#     print(f"Files to combine: {file_list}")
#     print(f"Global fuzzy columns: {fuzzy_columns}")
#     print(f"Global exact columns: {exact_columns}")
    
#     output_combined_file = os.path.join(output_dir, 'All_Final_Sheets_Combined.xlsx')
#     final_winners = []
#     merged_rows = pd.DataFrame()

#     with pd.ExcelWriter(output_combined_file) as combined_writer:
#         for file in file_list:
#             # Read the already processed file (not reprocess it)
#             source_system = os.path.splitext(os.path.basename(file))[0].split('_Output')[0]
#             print(f"Reading final sheet from: {file} for source system: {source_system}")

#             try:
#                 df_final = pd.read_excel(file, sheet_name=f'{source_system}_final'[:31])
#                 df_final['Source_System'] = source_system  # Add source system column
#                 df_final.to_excel(combined_writer, sheet_name=f'{source_system}_final'[:31], index=False)
#                 merged_rows = pd.concat([merged_rows, df_final], ignore_index=True)
#                 final_winners.append(source_system)
#                 print(f"Added {len(df_final)} rows from {source_system}")
#             except Exception as e:
#                 print(f"Error reading final sheet from {file}: {e}")

#         if not merged_rows.empty:
#             merged_rows.to_excel(combined_writer, sheet_name='crosssystem_input', index=False)
#             print(f"Combined {len(merged_rows)} total rows for cross-system processing")

#     print(f"Combined file saved to: {output_combined_file}")
#     return final_winners, output_combined_file


# def generate_cross_system_winner(combined_excel_file, rulebook, fuzzy_columns, exact_columns, fuzzy_thresholds, source_system_main_file, output_dir):
#     print(f"\n=== GENERATING CROSS-SYSTEM WINNERS ===")
#     print(f"Input file: {combined_excel_file}")
#     print(f"Fuzzy columns: {fuzzy_columns}")
#     print(f"Exact columns: {exact_columns}")
    
#     df = pd.read_excel(combined_excel_file, sheet_name='crosssystem_input')
#     print(f"Cross-system input data shape: {df.shape}")
#     print(f"Source systems in data: {df['Source_System'].unique()}")
    
#     df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)

#     duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
#     unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
    
#     print(f"Cross-system duplicates: {len(duplicate_rows)}, Unique: {len(unique_rows)}")

#     duplicate_rows = assign_winner(duplicate_rows, 'cross', rulebook, is_cross_system=True, source_system_main_file=source_system_main_file)
#     winner_rows = duplicate_rows[duplicate_rows['Source_System'] == duplicate_rows['winner_source']].copy()

#     final_rows = pd.concat([winner_rows, unique_rows], ignore_index=True)
#     output_path = os.path.join(output_dir, 'CrossSystem_Winner_Output.xlsx')

#     with pd.ExcelWriter(output_path) as writer:
#         final_rows.to_excel(writer, sheet_name="crosssystem_final", index=False)
#         winner_rows.to_excel(writer, sheet_name="winners_only", index=False)
#         duplicate_rows.to_excel(writer, sheet_name="all_duplicates", index=False)
#         unique_rows.to_excel(writer, sheet_name="uniques", index=False)

#     print(f"Cross-system output saved to: {output_path}")
#     print(f"Final cross-system results: {len(final_rows)} total rows")
#     return output_path


import pandas as pd
import os
import random
from itertools import combinations

# RapidFuzz Library Optimization
try:
    from rapidfuzz import fuzz
    RAPIDFUZZ_AVAILABLE = True
    print("✅ Using rapidfuzz for 5-10x faster fuzzy matching")
except ImportError:
    from fuzzywuzzy import fuzz
    RAPIDFUZZ_AVAILABLE = False
    print("⚠️ Install rapidfuzz for better performance: pip install rapidfuzz")
print("your_existing_script.py loaded")

def preprocess_data_for_speed(df, fuzzy_columns, exact_columns):
    """
    Data Preprocessing Optimization
    Clean and standardize data for faster comparisons
    """
    print("🔧 Preprocessing data for faster matching...")
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Get all columns that will be used for matching
    all_matching_columns = list(set(fuzzy_columns + exact_columns))
    
    # Clean and standardize data efficiently
    for col in all_matching_columns:
        if col in df.columns:
            # Fill NaN values, convert to string, strip whitespace, and convert to uppercase
            df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
    
    # Pre-calculate string lengths for fuzzy columns (used in length-based filtering)
    string_lengths = {}
    for col in fuzzy_columns:
        if col in df.columns:
            string_lengths[col] = df[col].str.len()
    
    print(f"✅ Preprocessed {len(all_matching_columns)} matching columns")
    return df, string_lengths


def length_based_prefilter(val1, val2, threshold=90):
    """
    Length-Based Pre-filtering Optimization
    Quick check before expensive fuzzy matching
    """
    # Quick exact match check
    if val1 == val2:
        return True, 100
    
    # Handle empty strings
    if not val1 or not val2:
        return (not val1 and not val2), (100 if (not val1 and not val2) else 0)
    
    # Length-based pre-filtering
    len1, len2 = len(val1), len(val2)
    if len1 == 0 and len2 == 0:
        return True, 100
    if len1 == 0 or len2 == 0:
        return False, 0
    
    # Calculate length ratio
    length_ratio = (min(len1, len2) / max(len1, len2)) * 100
    
    # If length difference is too large, skip expensive fuzzy matching
    # Use conservative threshold to avoid false negatives
    if length_ratio < threshold - 25:
        return False, 0
    
    # Passed pre-filter, proceed with fuzzy matching
    return True, None


def fast_fuzzy_ratio(val1, val2, threshold=90):
    """
    Optimized fuzzy matching with pre-filtering
    """
    # Apply length-based pre-filtering first
    should_proceed, quick_score = length_based_prefilter(val1, val2, threshold)
    
    if not should_proceed:
        return quick_score
    if quick_score is not None:  # Exact match found
        return quick_score
    
    # Proceed with fuzzy matching using RapidFuzz or FuzzyWuzzy
    return fuzz.ratio(val1, val2)


def union_find_grouping(matches):
    """
    Union-Find for Grouping Optimization
    Efficient group assignment using Union-Find data structure
    """
    print("🔗 Using Union-Find for efficient group assignment...")
    
    # Union-Find data structure
    parent = {}
    
    def find(x):
        """Find root of element x with path compression"""
        if x not in parent:
            parent[x] = x
        if parent[x] != x:
            parent[x] = find(parent[x])  # Path compression
        return parent[x]
    
    def union(x, y):
        """Union two elements into the same group"""
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py
    
    # Process all matches to build groups
    for idx_a, idx_b, overall_score, match_scores in matches:
        union(idx_a, idx_b)
    
    # Create group mapping
    group_mapping = {}
    group_id = 1
    
    # Assign group IDs
    all_indices = set()
    for idx_a, idx_b, _, _ in matches:
        all_indices.add(idx_a)
        all_indices.add(idx_b)
    
    for idx in all_indices:
        root = find(idx)
        if root not in group_mapping:
            group_mapping[root] = group_id
            group_id += 1
    
    # Return final group assignments
    final_groups = {}
    for idx in all_indices:
        root = find(idx)
        final_groups[idx] = group_mapping[root]
    
    return final_groups, group_id

def get_rule_matches(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90, rule_id=None):
    """
    Run a single rule and return matches as list of tuples
    Each tuple: (idx_a, idx_b, overall_score, match_scores)
    """
    df, _ = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
    all_matches = []

    for a, b in combinations(df.index, 2):
        match_scores = {}

        # fuzzy matching
        for column in fuzzy_columns:
            threshold = fuzzy_thresholds.get(column, 90)
            val_a, val_b = str(df.at[a, column]), str(df.at[b, column])
            score = fast_fuzzy_ratio(val_a, val_b, threshold)
            match_scores[column] = score
            if score < threshold:
                break  # early exit

        # check if all fuzzy passed
        if all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
            # check exact columns
            exact_match = all(df.at[a, col] == df.at[b, col] for col in exact_columns)
            overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0

            if exact_match and overall_score >= exact_threshold:
                all_matches.append((a, b, overall_score, match_scores,rule_id))

    return all_matches

def find_fuzzy_duplicates_multi_rules(df, rules, exact_threshold=90):
    """
    Process multiple rules:
      - Run get_rule_matches for each rule independently
      - Combine all matches
      - Apply Union-Find ONCE to handle transitivity
    """
    print(f"Processing {len(rules)} rules for duplicate detection...")

    all_matches = []
    for i, rule in enumerate(rules, start=1):
        fuzzy_cols = rule.get("fuzzy_columns", [])
        exact_cols = rule.get("exact_columns", [])
        thresholds = rule.get("thresholds", {})
        print(f"▶ Rule {i}: fuzzy={fuzzy_cols}, exact={exact_cols}, thresholds={thresholds}")

        matches = get_rule_matches(df, fuzzy_cols, exact_cols, thresholds, exact_threshold,rule_id=f"Rule_{i}")
        print(f"   Rule {i} matches found: {len(matches)}")
        all_matches.extend(matches)

    print(f"Total combined matches from all rules: {len(all_matches)}")

    if "matched_rules" not in df.columns:
        df["matched_rules"] = ""

    # Assign group IDs with union-find
    if all_matches:
        matches_for_union_find = [(a, b, s, m) for (a, b, s, m, r) in all_matches]
        groups, next_group_id = union_find_grouping(matches_for_union_find)


        for idx_a, idx_b, overall_score, match_scores, rule_id in all_matches:
            # Update match percentage
            df.at[idx_a, "match_percentage"] = float(overall_score)
            df.at[idx_b, "match_percentage"] = float(overall_score)


            # Update per-column fuzzy percentages
            for col, score in match_scores.items():
                col_name = f"{col}_fuzzy_match_percentage"
                if col_name not in df.columns:
                    df[col_name] = 0.0
                df.at[idx_a, col_name] = score
                df.at[idx_b, col_name] = score

            for idx in [idx_a, idx_b]:
                if pd.isnull(df.at[idx, "matched_rules"]) or df.at[idx, "matched_rules"] == "":
                    df.at[idx, "matched_rules"] = rule_id
                else:
                    existing = str(df.at[idx, "matched_rules"])
                    if rule_id not in existing.split(","):
                        df.at[idx, "matched_rules"] = existing + "," + rule_id

        for index, group_id in groups.items():
            df.at[index, "group_id"] = group_id

        group_id = next_group_id
    else:
        group_id = 1

    # Assign unique group IDs for unmatched rows
    for index, row in df.iterrows():
        if pd.isnull(row.get("group_id")):
            df.at[index, "group_id"] = group_id
            group_id += 1

    duplicate_groups = len([g for g in df["group_id"].value_counts() if g > 1])
    print(f"✅ Found {duplicate_groups} duplicate groups across all rules")

    return df

def find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90):
    print(f"Finding duplicates with fuzzy_columns: {fuzzy_columns}, exact_columns: {exact_columns}")
    
    # Data Preprocessing Optimization
    df, string_lengths = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
    
    # Initialize result columns
    df['group_id'] = None
    df['match_percentage'] = 0.0
    for column in fuzzy_columns:
        df[f'{column}_fuzzy_match_percentage'] = 0.0

    # Store all matches for Union-Find processing
    all_matches = []
    
    # Compare all pairs (keeping original logic but with optimizations)
    total_comparisons = 0
    for a, b in combinations(df.index, 2):
        total_comparisons += 1
        
        # Progress indicator for large datasets
        if total_comparisons % 50000 == 0:
            print(f"   Processed {total_comparisons:,} comparisons...")
        
        match_scores = {}
        
        # Fuzzy matching with optimizations
        for column in fuzzy_columns:
            threshold = fuzzy_thresholds.get(column, 90)
            
            # Get values
            val_a = str(df.at[a, column])
            val_b = str(df.at[b, column])
            
            # Use optimized fuzzy matching with length pre-filtering
            match_score = fast_fuzzy_ratio(val_a, val_b, threshold)
            match_scores[column] = match_score
            
            # Early termination if any fuzzy column fails
            if match_score < threshold:
                break

        # Check if all fuzzy columns passed
        if all(match_scores[column] >= fuzzy_thresholds.get(column, 90) for column in fuzzy_columns):
            # Check exact columns
            exact_match = all(df.at[a, col] == df.at[b, col] for col in exact_columns)
            overall_match_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0

            if exact_match and overall_match_score >= exact_threshold:
                # Store match for Union-Find processing
                all_matches.append((a, b, overall_match_score, match_scores))

    print(f"✅ Completed {total_comparisons:,} comparisons, found {len(all_matches):,} matches")
    
    # Union-Find for Grouping Optimization
    if all_matches:
        groups, next_group_id = union_find_grouping(all_matches)
        
        # Assign group IDs and match scores
        for idx_a, idx_b, overall_score, match_scores in all_matches:
            # Update match percentages
            df.at[idx_a, 'match_percentage'] = float(overall_score)
            df.at[idx_b, 'match_percentage'] = float(overall_score)
            
            # Update individual fuzzy match percentages
            for column in fuzzy_columns:
                if column in match_scores:
                    df.at[idx_a, f'{column}_fuzzy_match_percentage'] = match_scores[column]
                    df.at[idx_b, f'{column}_fuzzy_match_percentage'] = match_scores[column]
        
        # Assign group IDs from Union-Find results
        for index, group_id in groups.items():
            df.at[index, 'group_id'] = group_id
        
        group_id = next_group_id
    else:
        group_id = 1

    # Assign unique group IDs to unmatched records
    for index, row in df.iterrows():
        if pd.isnull(row['group_id']):
            df.at[index, 'group_id'] = group_id
            group_id += 1

    duplicate_groups = len([g for g in df['group_id'].value_counts() if g > 1])
    print(f"Found {duplicate_groups} duplicate groups using optimized Union-Find")
    return df

def assign_winner(df, source_system, rulebook, is_cross_system=False, source_system_main_file=None):
    print(f"Assigning winners for source_system: {source_system}, is_cross_system: {is_cross_system}")
    
    # Handle different possible transaction date column names
    transaction_date_col = None
    possible_date_names = ['Transaction Date', 'Transaction_Date', 'transaction_date', 'TransactionDate', 'Date', 'date']
    
    for col_name in possible_date_names:
        if col_name in df.columns:
            transaction_date_col = col_name
            break
    
    if transaction_date_col is None:
        print("⚠️ No transaction date column found, using row index as fallback")
        df['Transaction_Date_Fallback'] = pd.to_datetime('2023-01-01') + pd.to_timedelta(df.index, unit='D')
        transaction_date_col = 'Transaction_Date_Fallback'
    
    # Convert to datetime
    df[transaction_date_col] = pd.to_datetime(df[transaction_date_col], errors='coerce')
    df['winner'] = None

    if not is_cross_system:
        criteria_row = rulebook[rulebook['source_system'] == source_system]
        if criteria_row.empty:
            print(f"WARNING: Source system {source_system} not found in rulebook. Using default criteria.")
            winning_criteria = 'latest_transaction_date'
        else:
            winning_criteria = criteria_row['winning_criteria'].values[0]

        print(f"Using winning criteria: {winning_criteria}")

        for group_id, group in df.groupby('group_id'):
            try:
                if winning_criteria == 'latest_transaction_date':
                    winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
                elif winning_criteria == 'earliest_transaction_date':
                    winner_id = group.sort_values(by=transaction_date_col, ascending=True).iloc[0]['Cust_Id']
                elif winning_criteria == 'largest_name':
                    # Handle different possible name column names
                    name_col = None
                    for col in ['first_name', 'First_Name', 'firstName', 'name']:
                        if col in df.columns:
                            name_col = col
                            break
                    
                    if name_col:
                        winner_id = group.loc[group[name_col].str.len().idxmax()]['Cust_Id']
                    else:
                        print(f"⚠️ No name column found for 'largest_name' criteria, using latest date")
                        winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
                else:
                    winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
                
                df.loc[df['group_id'] == group_id, 'winner'] = winner_id
            except Exception as e:
                print(f"Error selecting winner for group {group_id} in {source_system}: {e}")
    else:
        print("Processing cross-system winner selection...")
        df['winner_source'] = None
        for group_id, group in df.groupby('group_id'):
            try:
                group_priorities = group[['Cust_Id', 'Source_System']].rename(columns={'Source_System': 'source_system'}) \
                    .merge(source_system_main_file, on='source_system', how='left')

                group_priorities = group_priorities.sort_values(by='precedence')
                winner_id = group_priorities.iloc[0]['Cust_Id']
                winner_source = group_priorities.iloc[0]['source_system']

                df.loc[df['group_id'] == group_id, 'winner'] = winner_id
                df.loc[df['group_id'] == group_id, 'winner_source'] = winner_source
            except Exception as e:
                print(f"Error selecting cross-system winner for group {group_id}: {e}")

    return df


def process_excel_file(file_path, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
    print(f"\n=== PROCESSING FILE: {file_path} ===")
    print(f"Fuzzy columns: {fuzzy_columns}")
    print(f"Exact columns: {exact_columns}")
    print(f"Thresholds: {fuzzy_thresholds}")
    
    # Record start time for performance measurement
    import time
    start_time = time.time()
    
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip()
    original_columns = df.columns.tolist()
    print(f"Original columns: {original_columns}")
    print(f"Data shape: {df.shape}")

    source_system = os.path.splitext(os.path.basename(file_path))[0]
    source_system_rule = source_system.split('_')[0]
    print(f"Source system: {source_system}, Rule system: {source_system_rule}")

    # Apply optimized fuzzy duplicate detection
    df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)

    duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
    unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
    
    print(f"Duplicate rows: {len(duplicate_rows)}, Unique rows: {len(unique_rows)}")

    duplicate_rows = assign_winner(duplicate_rows, source_system_rule, rulebook, is_cross_system=False)
    winner_rows = duplicate_rows[duplicate_rows['Cust_Id'] == duplicate_rows['winner']].copy()

    final_rows = pd.concat([winner_rows[original_columns], unique_rows[original_columns]], ignore_index=True)
    print(f"Final rows count: {len(final_rows)}")

    output_excel_file_name = f'{source_system}_Output.xlsx'
    output_path = os.path.join(output_dir, output_excel_file_name)

    with pd.ExcelWriter(output_path) as writer:
        final_rows.to_excel(writer, sheet_name=f'{source_system}_final'[:31], index=False)
        winner_rows.to_excel(writer, sheet_name=f'{source_system}_winner'[:31], index=False)
        duplicate_rows.to_excel(writer, sheet_name=f'{source_system}_duplicates'[:31], index=False)
        unique_rows.to_excel(writer, sheet_name=f'{source_system}_unique'[:31], index=False)

    # Performance summary
    total_time = time.time() - start_time
    print(f"Output saved to: {output_path}")
    print(f"⚡ Processing completed in {total_time:.2f} seconds")
    if total_time > 0:
        print(f"⚡ Processing rate: {len(df) / total_time:.0f} records/second")
    
    return output_path


def process_all_and_combine_final_sheets(file_list, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
    print(f"\n=== COMBINING FINAL SHEETS ===")
    print(f"Files to combine: {file_list}")
    print(f"Global fuzzy columns: {fuzzy_columns}")
    print(f"Global exact columns: {exact_columns}")
    
    output_combined_file = os.path.join(output_dir, 'All_Final_Sheets_Combined.xlsx')
    final_winners = []
    merged_rows = pd.DataFrame()

    with pd.ExcelWriter(output_combined_file) as combined_writer:
        for file in file_list:
            # Read the already processed file (not reprocess it)
            source_system = os.path.splitext(os.path.basename(file))[0].split('_Output')[0]
            print(f"Reading final sheet from: {file} for source system: {source_system}")

            try:
                df_final = pd.read_excel(file, sheet_name=f'{source_system}_final'[:31])
                df_final['Source_System'] = source_system  # Add source system column
                df_final.to_excel(combined_writer, sheet_name=f'{source_system}_final'[:31], index=False)
                merged_rows = pd.concat([merged_rows, df_final], ignore_index=True)
                final_winners.append(source_system)
                print(f"Added {len(df_final)} rows from {source_system}")
            except Exception as e:
                print(f"Error reading final sheet from {file}: {e}")

        if not merged_rows.empty:
            merged_rows.to_excel(combined_writer, sheet_name='crosssystem_input', index=False)
            print(f"Combined {len(merged_rows)} total rows for cross-system processing")

    print(f"Combined file saved to: {output_combined_file}")
    return final_winners, output_combined_file


def generate_cross_system_winner(combined_excel_file, rulebook, fuzzy_columns, exact_columns, fuzzy_thresholds, source_system_main_file, output_dir):
    print(f"\n=== GENERATING CROSS-SYSTEM WINNERS ===")
    print(f"Input file: {combined_excel_file}")
    print(f"Fuzzy columns: {fuzzy_columns}")
    print(f"Exact columns: {exact_columns}")
    
    import time
    start_time = time.time()
    
    df = pd.read_excel(combined_excel_file, sheet_name='crosssystem_input')
    print(f"Cross-system input data shape: {df.shape}")
    print(f"Source systems in data: {df['Source_System'].unique()}")
    
    # Apply optimized fuzzy duplicate detection
    df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)

    duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
    unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
    
    print(f"Cross-system duplicates: {len(duplicate_rows)}, Unique: {len(unique_rows)}")

    duplicate_rows = assign_winner(duplicate_rows, 'cross', rulebook, is_cross_system=True, source_system_main_file=source_system_main_file)
    winner_rows = duplicate_rows[duplicate_rows['Source_System'] == duplicate_rows['winner_source']].copy()

    final_rows = pd.concat([winner_rows, unique_rows], ignore_index=True)
    output_path = os.path.join(output_dir, 'CrossSystem_Winner_Output.xlsx')

    with pd.ExcelWriter(output_path) as writer:
        final_rows.to_excel(writer, sheet_name="crosssystem_final", index=False)
        winner_rows.to_excel(writer, sheet_name="winners_only", index=False)
        duplicate_rows.to_excel(writer, sheet_name="all_duplicates", index=False)
        unique_rows.to_excel(writer, sheet_name="uniques", index=False)

    total_time = time.time() - start_time
    print(f"Cross-system output saved to: {output_path}")
    print(f"Final cross-system results: {len(final_rows)} total rows")
    print(f"⚡ Cross-system processing completed in {total_time:.2f} seconds")
    return output_path

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({"error": "File too large"}), 413

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 EY DEDUPLICATION ENGINE v2.0 - STARTING UP")
    print("="*60)
    
    print(f"\n📁 Directory Configuration:")
    print(f"   Data directory: {os.path.abspath(DATA_DIR)}")
    print(f"   Static directory: {os.path.abspath(STATIC_DIR)}")
    print(f"   Output directory: {os.path.abspath(OUTPUT_DIR)}")
    print(f"   Processed outputs directory: {os.path.abspath(PROCESSED_OUTPUTS_DIR)}")
    
    # Create directories if they don't exist
    created_dirs = []
    for directory in [DATA_DIR, STATIC_DIR, OUTPUT_DIR, PROCESSED_OUTPUTS_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            created_dirs.append(directory)
    
    if created_dirs:
        print(f"\n✅ Created directories: {', '.join(created_dirs)}")
    
    # Initialize processed outputs registry if it doesn't exist
    registry_file = os.path.join(PROCESSED_OUTPUTS_DIR, 'registry.json')
    if not os.path.exists(registry_file):
        save_processed_outputs_registry({})
        print("✅ Initialized processed outputs registry")
    
    # Check for required files
    required_files = [
        ('Rulebook.xlsx', os.path.join(STATIC_DIR, 'Rulebook.xlsx')),
        ('Source_System_Mapping.xlsx', os.path.join(STATIC_DIR, 'Source_System_Mapping.xlsx'))
    ]
    
    missing_files = []
    existing_files = []
    
    for name, file_path in required_files:
        if os.path.exists(file_path):
            existing_files.append(name)
        else:
            missing_files.append(name)
    
    if existing_files:
        print(f"✅ Found required files: {', '.join(existing_files)}")
    
    if missing_files:
        print(f"\n⚠️  WARNING: Missing required files: {', '.join(missing_files)}")
        print("   Please add these files to the static_data directory for full functionality")
    
    # Check system resources
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('.')
    
    print(f"\n💻 System Resources:")
    print(f"   Memory: {memory.available / 1024 / 1024 / 1024:.1f}GB available / {memory.total / 1024 / 1024 / 1024:.1f}GB total")
    print(f"   Disk: {disk.free / 1024 / 1024 / 1024:.1f}GB free / {disk.total / 1024 / 1024 / 1024:.1f}GB total")
    
    # Count existing data
    entities_count = 0
    total_files = 0
    
    if os.path.exists(DATA_DIR):
        try:
            entities = [d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))]
            entities_count = len(entities)
            
            for entity in entities:
                entity_path = os.path.join(DATA_DIR, entity)
                if os.path.isdir(entity_path):
                    for item in os.listdir(entity_path):
                        source_path = os.path.join(entity_path, item)
                        if os.path.isdir(source_path):
                            files = [f for f in os.listdir(source_path) if f.endswith(('.xlsx', '.xls'))]
                            total_files += len(files)
        except Exception as e:
            print(f"   Warning: Error scanning data directory: {e}")
    
    print(f"\n📊 Data Statistics:")
    print(f"   Entities: {entities_count}")
    print(f"   Source files: {total_files}")
    
    if os.path.exists(OUTPUT_DIR):
        output_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(('.xlsx', '.xls'))]
        print(f"   Output files: {len(output_files)}")
    
    print(f"\n🌐 Server Configuration:")
    print(f"   Host: 0.0.0.0 (accessible from network)")
    print(f"   Port: 5000")
    print(f"   Debug mode: True")
    
    print(f"\n📡 API Endpoints:")
    print(f"   Health check: http://localhost:5000/api/health")
    print(f"   System info: http://localhost:5000/api/system-info")
    print(f"   Performance benchmark: http://localhost:5000/api/performance-benchmark")
    
    print(f"\n🎯 Frontend:")
    print(f"   React app should run on: http://localhost:3000")
    print(f"   Make sure to run 'npm start' in the frontend directory")
    
    print("\n" + "="*60)
    print("🎉 READY TO PROCESS YOUR 50,000 RECORD FILES!")
    print("="*60 + "\n")
    
    # Start the Flask application
    try:
        app.run(debug=True, host='0.0.0.0', port=5001, threaded=True)
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down EY Deduplication Engine...")
        print("Thank you for using the system!")
    except Exception as e:
        print(f"\n❌ Error starting server: {e}")
        print("Please check the configuration and try again.")
