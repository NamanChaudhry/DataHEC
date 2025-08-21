# app.py - Complete Flask Backend (Full Version)
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import time
import psutil
import os
import pandas as pd
import json
from datetime import datetime

# Import your existing deduplication functions
try:
    from your_existing_script import (
        process_all_and_combine_final_sheets,
        generate_cross_system_winner,
        process_excel_file,
        find_fuzzy_duplicates,
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
def process_excel_file_with_stats(file_path, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
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

# API Routes
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
                filepath, fuzzy_columns, exact_columns, thresholds, rulebook, OUTPUT_DIR
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
