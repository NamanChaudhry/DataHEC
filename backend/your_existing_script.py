
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