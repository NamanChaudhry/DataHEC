# # import pandas as pd
# # import os
# # import random
# # from itertools import combinations
# # from tqdm import tqdm
# # from functools import partial


# # # RapidFuzz Library Optimization
# # try:
# #     from rapidfuzz import fuzz
# #     RAPIDFUZZ_AVAILABLE = True
# #     print("✅ Using rapidfuzz for 5-10x faster fuzzy matching")
# # except ImportError:
# #     from fuzzywuzzy import fuzz
# #     RAPIDFUZZ_AVAILABLE = False
# #     print("⚠️ Install rapidfuzz for better performance: pip install rapidfuzz")
# # print("your_existing_script.py loaded")

# # import pandas as pd
# # import re
# # from itertools import combinations
# # from collections import defaultdict
# # import jellyfish


# # def generate_pair_chunks(idx_list, chunk_size=200_000):
# #     n = len(idx_list)
# #     batch = []
# #     count = 0
# #     for i_pos in range(n-1):
# #         a = idx_list[i_pos]
# #         for j_pos in range(i_pos+1, n):
# #             b = idx_list[j_pos]
# #             batch.append((a, b))
# #             count += 1
# #             if count >= chunk_size:
# #                 yield batch
# #                 batch = []
# #                 count = 0
# #     if batch:
# #         yield batch

# # def process_chunk_worker(chunk_pairs, df_values, fuzzy_columns, fuzzy_thresholds):
# #     """
# #     Worker for parallel all-pairs fuzzy comparison.
# #     df_values: dict of column -> list/array of normalized strings (indexed by df.index)
# #     """
# #     res = []
# #     for a, b in chunk_pairs:
# #         match_scores = {}
# #         passed = True
# #         for col in fuzzy_columns:
# #             threshold = fuzzy_thresholds.get(col, 90)
# #             val_a, val_b = str(df_values[col][a]), str(df_values[col][b])
# #             score = fast_fuzzy_ratio(val_a, val_b, threshold)
# #             match_scores[col] = score
# #             if score < threshold:
# #                 passed = False
# #                 break
# #         if passed and all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
# #             overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0
# #             res.append((a, b, overall_score, match_scores))
# #     return res


# # def normalize_string(s: str) -> str:
# #     if not s:
# #         return ""
# #     s = s.lower().strip()
# #     s = re.sub(r'\s+', ' ', s)
# #     return s


# # def token_sort_block_key(s: str) -> str:
# #     tokens = re.split(r'\s+', s)
# #     tokens.sort()
# #     return ' '.join(tokens)

# # def phonetic_block_keys(s: str) -> list:
# #     tokens = re.split(r'\s+', s)
# #     return [jellyfish.metaphone(token) for token in tokens]

# # def ngram_block_keys(s: str, n: int = 2) -> list:
# #     s = re.sub(r'\s+', '', s)
# #     return [s[i:i+n] for i in range(len(s)-n+1)] if len(s) >= n else [s]


# # def generate_fuzzy_blocks(df, column):
# #     blocks = defaultdict(list)
# #     for idx, val in df[column].items():
# #         val = normalize_string(str(val))
# #         if not val:
# #             continue
# #         # Token-Sort
# #         blocks[f"TOKEN_{token_sort_block_key(val)}"].append(idx)
# #         # Phonetic
# #         for p in phonetic_block_keys(val):
# #             blocks[f"PHON_{p}"].append(idx)
# #         # N-Grams
# #         for n in ngram_block_keys(val, n=2):
# #             blocks[f"NGRAM_{n}"].append(idx)
# #     return blocks

# # def generate_exact_blocks(df, exact_columns):
# #     from collections import defaultdict
# #     blocks = defaultdict(list)
# #     for idx, row in df.iterrows():
# #         key = tuple(row[col] for col in exact_columns)
# #         blocks[key].append(idx)
# #     return blocks

# # #Candidate pair generation with exact blocking + fuzzy matching within blocks


# # def preprocess_data_for_speed(df, fuzzy_columns, exact_columns):
# #     """
# #     Data Preprocessing Optimization
# #     Clean and standardize data for faster comparisons
# #     """
# #     print("🔧 Preprocessing data for faster matching...")
    
# #     # Make a copy to avoid modifying original
# #     df = df.copy()
    
# #     # Get all columns that will be used for matching
# #     all_matching_columns = list(set(fuzzy_columns + exact_columns))
    
# #     # Clean and standardize data efficiently
# #     for col in all_matching_columns:
# #         if col in df.columns:
# #             # Fill NaN values, convert to string, strip whitespace, and convert to uppercase
# #             df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
    
# #     # Pre-calculate string lengths for fuzzy columns (used in length-based filtering)
# #     string_lengths = {}
# #     for col in fuzzy_columns:
# #         if col in df.columns:
# #             string_lengths[col] = df[col].str.len()
    
# #     print(f"✅ Preprocessed {len(all_matching_columns)} matching columns")
# #     return df, string_lengths


# # def length_based_prefilter(val1, val2, threshold=90):
# #     """
# #     Length-Based Pre-filtering Optimization
# #     Quick check before expensive fuzzy matching
# #     """
# #     # Quick exact match check
# #     if val1 == val2:
# #         return True, 100
    
# #     # Handle empty strings
# #     if not val1 or not val2:
# #         return (not val1 and not val2), (100 if (not val1 and not val2) else 0)
    
# #     # Length-based pre-filtering
# #     len1, len2 = len(val1), len(val2)
# #     if len1 == 0 and len2 == 0:
# #         return True, 100
# #     if len1 == 0 or len2 == 0:
# #         return False, 0
    
# #     # Calculate length ratio
# #     length_ratio = (min(len1, len2) / max(len1, len2)) * 100
    
# #     # If length difference is too large, skip expensive fuzzy matching
# #     # Use conservative threshold to avoid false negatives
# #     if length_ratio < threshold - 25:
# #         return False, 0
    
# #     # Passed pre-filter, proceed with fuzzy matching
# #     return True, None


# # def fast_fuzzy_ratio(val1, val2, threshold=90):
# #     """
# #     Optimized fuzzy matching with pre-filtering
# #     """
# #     # Apply length-based pre-filtering first
# #     should_proceed, quick_score = length_based_prefilter(val1, val2, threshold)
    
# #     if not should_proceed:
# #         return quick_score
# #     if quick_score is not None:  # Exact match found
# #         return quick_score
    
# #     # Proceed with fuzzy matching using RapidFuzz or FuzzyWuzzy
# #     return fuzz.ratio(val1, val2)


# # def union_find_grouping(matches):
# #     """
# #     Union-Find for Grouping Optimization
# #     Efficient group assignment using Union-Find data structure
# #     """
# #     print("🔗 Using Union-Find for efficient group assignment...")
    
# #     # Union-Find data structure
# #     parent = {}
    
# #     def find(x):
# #         """Find root of element x with path compression"""
# #         if x not in parent:
# #             parent[x] = x
# #         if parent[x] != x:
# #             parent[x] = find(parent[x])  # Path compression
# #         return parent[x]
    
# #     def union(x, y):
# #         """Union two elements into the same group"""
# #         px, py = find(x), find(y)
# #         if px != py:
# #             parent[px] = py
    
# #     # Process all matches to build groups
# #     for idx_a, idx_b, overall_score, match_scores in matches:
# #         union(idx_a, idx_b)
    
# #     # Create group mapping
# #     group_mapping = {}
# #     group_id = 1
    
# #     # Assign group IDs
# #     all_indices = set()
# #     for idx_a, idx_b, _, _ in matches:
# #         all_indices.add(idx_a)
# #         all_indices.add(idx_b)
    
# #     for idx in all_indices:
# #         root = find(idx)
# #         if root not in group_mapping:
# #             group_mapping[root] = group_id
# #             group_id += 1
    
# #     # Return final group assignments
# #     final_groups = {}
# #     for idx in all_indices:
# #         root = find(idx)
# #         final_groups[idx] = group_mapping[root]
    
# #     return final_groups, group_id

# # def get_rule_matches(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90, rule_id=None):
# #     """
# #     Run a single rule and return matches as list of tuples
# #     Each tuple: (idx_a, idx_b, overall_score, match_scores)
# #     """
# #     df, _ = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
# #     all_matches = []

# #     for a, b in combinations(df.index, 2):
# #         match_scores = {}

# #         # fuzzy matching
# #         for column in fuzzy_columns:
# #             threshold = fuzzy_thresholds.get(column, 90)
# #             val_a, val_b = str(df.at[a, column]), str(df.at[b, column])
# #             score = fast_fuzzy_ratio(val_a, val_b, threshold)
# #             match_scores[column] = score
# #             if score < threshold:
# #                 break  # early exit

# #         # check if all fuzzy passed
# #         if all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
# #             # check exact columns
# #             exact_match = all(df.at[a, col] == df.at[b, col] for col in exact_columns)
# #             overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0

# #             if exact_match and overall_score >= exact_threshold:
# #                 all_matches.append((a, b, overall_score, match_scores,rule_id))

# #     return all_matches

# # def find_fuzzy_duplicates_multi_rules(df, rules, exact_threshold=90):
# #     """
# #     Process multiple rules:
# #       - Run get_rule_matches for each rule independently
# #       - Combine all matches
# #       - Apply Union-Find ONCE to handle transitivity
# #     """
# #     print(f"Processing {len(rules)} rules for duplicate detection...")

# #     all_matches = []
# #     for i, rule in enumerate(rules, start=1):
# #         fuzzy_cols = rule.get("fuzzy_columns", [])
# #         exact_cols = rule.get("exact_columns", [])
# #         thresholds = rule.get("thresholds", {})
# #         print(f"▶ Rule {i}: fuzzy={fuzzy_cols}, exact={exact_cols}, thresholds={thresholds}")

# #         matches = get_rule_matches(df, fuzzy_cols, exact_cols, thresholds, exact_threshold,rule_id=f"Rule_{i}")
# #         print(f"   Rule {i} matches found: {len(matches)}")
# #         all_matches.extend(matches)

# #     print(f"Total combined matches from all rules: {len(all_matches)}")

# #     if "matched_rules" not in df.columns:
# #         df["matched_rules"] = ""

# #     # Assign group IDs with union-find
# #     if all_matches:
# #         matches_for_union_find = [(a, b, s, m) for (a, b, s, m, r) in all_matches]
# #         groups, next_group_id = union_find_grouping(matches_for_union_find)


# #         for idx_a, idx_b, overall_score, match_scores, rule_id in all_matches:
# #             # Update match percentage
# #             df.at[idx_a, "match_percentage"] = float(overall_score)
# #             df.at[idx_b, "match_percentage"] = float(overall_score)


# #             # Update per-column fuzzy percentages
# #             for col, score in match_scores.items():
# #                 col_name = f"{col}_fuzzy_match_percentage"
# #                 if col_name not in df.columns:
# #                     df[col_name] = 0.0
# #                 df.at[idx_a, col_name] = score
# #                 df.at[idx_b, col_name] = score

# #             for idx in [idx_a, idx_b]:
# #                 if pd.isnull(df.at[idx, "matched_rules"]) or df.at[idx, "matched_rules"] == "":
# #                     df.at[idx, "matched_rules"] = rule_id
# #                 else:
# #                     existing = str(df.at[idx, "matched_rules"])
# #                     if rule_id not in existing.split(","):
# #                         df.at[idx, "matched_rules"] = existing + "," + rule_id

# #         for index, group_id in groups.items():
# #             df.at[index, "group_id"] = group_id

# #         group_id = next_group_id
# #     else:
# #         group_id = 1

# #     # Assign unique group IDs for unmatched rows
# #     for index, row in df.iterrows():
# #         if pd.isnull(row.get("group_id")):
# #             df.at[index, "group_id"] = group_id
# #             group_id += 1

# #     duplicate_groups = len([g for g in df["group_id"].value_counts() if g > 1])
# #     print(f"✅ Found {duplicate_groups} duplicate groups across all rules")

# #     return df


# # def find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90):
# #     print(f"Finding duplicates with fuzzy_columns: {fuzzy_columns}, exact_columns: {exact_columns}")
    
# #     # Data Preprocessing Optimization
# #     df, string_lengths = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
    
# #     # Initialize result columns
# #     df['group_id'] = None
# #     df['match_percentage'] = 0.0
# #     for column in fuzzy_columns:
# #         df[f'{column}_fuzzy_match_percentage'] = 0.0

# #     # Store all matches for Union-Find processing
# #     all_matches = []
# #     # CASE 1: Only exact columns
# #     if fuzzy_columns == [] and exact_columns != []:
# #         exact_blocks = generate_exact_blocks(df, exact_columns)
# #         for block_indices in exact_blocks.values():
# #             if len(block_indices) < 2:
# #                 continue
# #             for a, b in combinations(block_indices, 2):
# #                 overall_score = 100.0
# #                 #all_matches.append((a, b, overall_score, {}, "Exact_Only"))
# #                 all_matches.append((a, b, overall_score, {}))

# #     # CASE 2: Only fuzzy columns
# #     # --- REPLACEMENT: Lossless, chunked, progress-aware all-pairs (plug into CASE 2) ---
# #     elif fuzzy_columns != [] and exact_columns == []:
# #         print("⚠️ Performing full all-pairs comparisons (lossless).")

# #         indices = list(df.index)
# #         n = len(indices)
# #         total_pairs = n*(n-1)//2
# #         print(f"Total all-pairs to evaluate: {total_pairs:,} (n={n})")

# #         CHUNK_SIZE = 200_000
# #         USE_PARALLEL = True

# #         # Precompute normalized strings for each fuzzy column to avoid repeated str(df.at[...])
# #         df_values = {col: df[col].to_dict() for col in fuzzy_columns}

# #         if not USE_PARALLEL:
# #             for chunk in tqdm(generate_pair_chunks(indices, CHUNK_SIZE), total=(total_pairs//CHUNK_SIZE)+1, desc="AllPairs"):
# #                 all_matches.extend(process_chunk_worker(chunk, df_values, fuzzy_columns, fuzzy_thresholds))
# #         else:
# #             import multiprocessing as mp
# #             cpu_count = max(1, mp.cpu_count() - 1)
# #             print(f"Using multiprocessing with {cpu_count} workers")
# #             pool = mp.Pool(cpu_count)
# #             try:
# #                 chunks = list(generate_pair_chunks(indices, CHUNK_SIZE))
# #                 total_chunks = len(chunks)
# #                 from functools import partial
# #                 worker_fn = partial(process_chunk_worker, df_values=df_values, fuzzy_columns=fuzzy_columns, fuzzy_thresholds=fuzzy_thresholds)
# #                 for chunk_result in tqdm(pool.imap_unordered(worker_fn, chunks), total=total_chunks, desc="ParallelAllPairs"):
# #                     if chunk_result:
# #                         all_matches.extend(chunk_result)
# #             finally:
# #                 pool.close()
# #                 pool.join()

# #         print(f"All-pairs evaluation complete. Candidate matches found: {len(all_matches)}")

# #     # CASE 3: Both fuzzy + exact columns
# #     elif fuzzy_columns != [] and exact_columns != []:
# #         exact_blocks = generate_exact_blocks(df, exact_columns)
# #         for block_indices in exact_blocks.values():
# #             if len(block_indices) < 2:
# #                 continue
# #             # Generate fuzzy blocks inside exact block
# #             for a, b in combinations(block_indices, 2):
# #                 match_scores = {}
# #                 for col in fuzzy_columns:
# #                     threshold = fuzzy_thresholds.get(col, 90)
# #                     val_a, val_b = str(df.at[a, col]), str(df.at[b, col])
# #                     score = fast_fuzzy_ratio(val_a, val_b, threshold)
# #                     match_scores[col] = score
# #                     if score < threshold:
# #                         break
# #                 if all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
# #                     overall_score = sum(match_scores.values()) / len(match_scores)
# #                     all_matches.append((a, b, overall_score, match_scores))
# #     print(f"Total candidate matches collected: {len(all_matches)}")


# #     # Union-Find for Grouping Optimization
# #     if all_matches:
# #         groups, next_group_id = union_find_grouping(all_matches)
        
# #         # Assign group IDs and match scores
# #         for idx_a, idx_b, overall_score, match_scores in all_matches:
# #             # Update match percentages
# #             df.at[idx_a, 'match_percentage'] = float(overall_score)
# #             df.at[idx_b, 'match_percentage'] = float(overall_score)
            
# #             # Update individual fuzzy match percentages
# #             for column in fuzzy_columns:
# #                 if column in match_scores:
# #                     df.at[idx_a, f'{column}_fuzzy_match_percentage'] = match_scores[column]
# #                     df.at[idx_b, f'{column}_fuzzy_match_percentage'] = match_scores[column]
        
# #         # Assign group IDs from Union-Find results
# #         for index, group_id in groups.items():
# #             df.at[index, 'group_id'] = group_id
        
# #         group_id = next_group_id
# #     else:
# #         group_id = 1

# #     # Assign unique group IDs to unmatched records
# #     for index, row in df.iterrows():
# #         if pd.isnull(row['group_id']):
# #             df.at[index, 'group_id'] = group_id
# #             group_id += 1

# #     duplicate_groups = len([g for g in df['group_id'].value_counts() if g > 1])
# #     print(f"Found {duplicate_groups} duplicate groups using optimized Union-Find")
# #     return df

# # def assign_winner(df, source_system, rulebook, is_cross_system=False, source_system_main_file=None):
# #     print(f"Assigning winners for source_system: {source_system}, is_cross_system: {is_cross_system}")
    
# #     # Handle different possible transaction date column names
# #     transaction_date_col = None
# #     possible_date_names = ['Transaction Date', 'Transaction_Date', 'transaction_date', 'TransactionDate', 'Date', 'date']
    
# #     for col_name in possible_date_names:
# #         if col_name in df.columns:
# #             transaction_date_col = col_name
# #             break
    
# #     if transaction_date_col is None:
# #         print("⚠️ No transaction date column found, using row index as fallback")
# #         df['Transaction_Date_Fallback'] = pd.to_datetime('2023-01-01') + pd.to_timedelta(df.index, unit='D')
# #         transaction_date_col = 'Transaction_Date_Fallback'
    
# #     # Convert to datetime
# #     df[transaction_date_col] = pd.to_datetime(df[transaction_date_col], errors='coerce')
# #     df['winner'] = None

# #     if not is_cross_system:
# #         criteria_row = rulebook[rulebook['source_system'] == source_system]
# #         if criteria_row.empty:
# #             print(f"WARNING: Source system {source_system} not found in rulebook. Using default criteria.")
# #             winning_criteria = 'latest_transaction_date'
# #         else:
# #             winning_criteria = criteria_row['winning_criteria'].values[0]

# #         print(f"Using winning criteria: {winning_criteria}")

# #         for group_id, group in df.groupby('group_id'):
# #             try:
# #                 if winning_criteria == 'latest_transaction_date':
# #                     winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
# #                 elif winning_criteria == 'earliest_transaction_date':
# #                     winner_id = group.sort_values(by=transaction_date_col, ascending=True).iloc[0]['Cust_Id']
# #                 elif winning_criteria == 'largest_name':
# #                     # Handle different possible name column names
# #                     name_col = None
# #                     for col in ['first_name', 'First_Name', 'firstName', 'name']:
# #                         if col in df.columns:
# #                             name_col = col
# #                             break
                    
# #                     if name_col:
# #                         winner_id = group.loc[group[name_col].str.len().idxmax()]['Cust_Id']
# #                     else:
# #                         print(f"⚠️ No name column found for 'largest_name' criteria, using latest date")
# #                         winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
# #                 else:
# #                     winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
                
# #                 df.loc[df['group_id'] == group_id, 'winner'] = winner_id
# #             except Exception as e:
# #                 print(f"Error selecting winner for group {group_id} in {source_system}: {e}")
# #     else:
# #         print("Processing cross-system winner selection...")
# #         df['winner_source'] = None
# #         for group_id, group in df.groupby('group_id'):
# #             try:
# #                 group_priorities = group[['Cust_Id', 'Source_System']].rename(columns={'Source_System': 'source_system'}) \
# #                     .merge(source_system_main_file, on='source_system', how='left')

# #                 group_priorities = group_priorities.sort_values(by='precedence')
# #                 winner_id = group_priorities.iloc[0]['Cust_Id']
# #                 winner_source = group_priorities.iloc[0]['source_system']

# #                 df.loc[df['group_id'] == group_id, 'winner'] = winner_id
# #                 df.loc[df['group_id'] == group_id, 'winner_source'] = winner_source
# #             except Exception as e:
# #                 print(f"Error selecting cross-system winner for group {group_id}: {e}")

# #     return df


# # def process_excel_file(file_path, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
# #     print(f"\n=== PROCESSING FILE: {file_path} ===")
# #     print(f"Fuzzy columns: {fuzzy_columns}")
# #     print(f"Exact columns: {exact_columns}")
# #     print(f"Thresholds: {fuzzy_thresholds}")
    
# #     # Record start time for performance measurement
# #     import time
# #     start_time = time.time()
    
# #     df = pd.read_excel(file_path)
# #     df.columns = df.columns.str.strip()
# #     original_columns = df.columns.tolist()
# #     print(f"Original columns: {original_columns}")
# #     print(f"Data shape: {df.shape}")

# #     source_system = os.path.splitext(os.path.basename(file_path))[0]
# #     source_system_rule = source_system.split('_')[0]
# #     print(f"Source system: {source_system}, Rule system: {source_system_rule}")

# #     # Apply optimized fuzzy duplicate detection
# #     df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)

# #     duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
# #     unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
    
# #     print(f"Duplicate rows: {len(duplicate_rows)}, Unique rows: {len(unique_rows)}")

# #     duplicate_rows = assign_winner(duplicate_rows, source_system_rule, rulebook, is_cross_system=False)
# #     winner_rows = duplicate_rows[duplicate_rows['Cust_Id'] == duplicate_rows['winner']].copy()

# #     final_rows = pd.concat([winner_rows[original_columns], unique_rows[original_columns]], ignore_index=True)
# #     print(f"Final rows count: {len(final_rows)}")

# #     output_excel_file_name = f'{source_system}_Output.xlsx'
# #     output_path = os.path.join(output_dir, output_excel_file_name)

# #     with pd.ExcelWriter(output_path) as writer:
# #         final_rows.to_excel(writer, sheet_name=f'{source_system}_final'[:31], index=False)
# #         winner_rows.to_excel(writer, sheet_name=f'{source_system}_winner'[:31], index=False)
# #         duplicate_rows.to_excel(writer, sheet_name=f'{source_system}_duplicates'[:31], index=False)
# #         unique_rows.to_excel(writer, sheet_name=f'{source_system}_unique'[:31], index=False)

# #     # Performance summary
# #     total_time = time.time() - start_time
# #     print(f"Output saved to: {output_path}")
# #     print(f"⚡ Processing completed in {total_time:.2f} seconds")
# #     if total_time > 0:
# #         print(f"⚡ Processing rate: {len(df) / total_time:.0f} records/second")
    
# #     return output_path


# # def process_all_and_combine_final_sheets(file_list, fuzzy_columns, exact_columns, fuzzy_thresholds, rulebook, output_dir):
# #     print(f"\n=== COMBINING FINAL SHEETS ===")
# #     print(f"Files to combine: {file_list}")
# #     print(f"Global fuzzy columns: {fuzzy_columns}")
# #     print(f"Global exact columns: {exact_columns}")
    
# #     output_combined_file = os.path.join(output_dir, 'All_Final_Sheets_Combined.xlsx')
# #     final_winners = []
# #     merged_rows = pd.DataFrame()

# #     with pd.ExcelWriter(output_combined_file) as combined_writer:
# #         for file in file_list:
# #             # Read the already processed file (not reprocess it)
# #             source_system = os.path.splitext(os.path.basename(file))[0].split('_Output')[0]
# #             print(f"Reading final sheet from: {file} for source system: {source_system}")

# #             try:
# #                 df_final = pd.read_excel(file, sheet_name=f'{source_system}_final'[:31])
# #                 df_final['Source_System'] = source_system  # Add source system column
# #                 df_final.to_excel(combined_writer, sheet_name=f'{source_system}_final'[:31], index=False)
# #                 merged_rows = pd.concat([merged_rows, df_final], ignore_index=True)
# #                 final_winners.append(source_system)
# #                 print(f"Added {len(df_final)} rows from {source_system}")
# #             except Exception as e:
# #                 print(f"Error reading final sheet from {file}: {e}")

# #         if not merged_rows.empty:
# #             merged_rows.to_excel(combined_writer, sheet_name='crosssystem_input', index=False)
# #             print(f"Combined {len(merged_rows)} total rows for cross-system processing")

# #     print(f"Combined file saved to: {output_combined_file}")
# #     return final_winners, output_combined_file


# # def generate_cross_system_winner(combined_excel_file, rulebook, fuzzy_columns, exact_columns, fuzzy_thresholds, source_system_main_file, output_dir):
# #     print(f"\n=== GENERATING CROSS-SYSTEM WINNERS ===")
# #     print(f"Input file: {combined_excel_file}")
# #     print(f"Fuzzy columns: {fuzzy_columns}")
# #     print(f"Exact columns: {exact_columns}")
    
# #     import time
# #     start_time = time.time()
    
# #     df = pd.read_excel(combined_excel_file, sheet_name='crosssystem_input')
# #     print(f"Cross-system input data shape: {df.shape}")
# #     print(f"Source systems in data: {df['Source_System'].unique()}")
    
# #     # Apply optimized fuzzy duplicate detection
# #     df = find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds)

# #     duplicate_rows = df[df.duplicated('group_id', keep=False)].copy()
# #     unique_rows = df[~df.duplicated('group_id', keep=False)].copy()
    
# #     print(f"Cross-system duplicates: {len(duplicate_rows)}, Unique: {len(unique_rows)}")

# #     duplicate_rows = assign_winner(duplicate_rows, 'cross', rulebook, is_cross_system=True, source_system_main_file=source_system_main_file)
# #     winner_rows = duplicate_rows[duplicate_rows['Source_System'] == duplicate_rows['winner_source']].copy()

# #     final_rows = pd.concat([winner_rows, unique_rows], ignore_index=True)
# #     output_path = os.path.join(output_dir, 'CrossSystem_Winner_Output.xlsx')

# #     with pd.ExcelWriter(output_path) as writer:
# #         final_rows.to_excel(writer, sheet_name="crosssystem_final", index=False)
# #         winner_rows.to_excel(writer, sheet_name="winners_only", index=False)
# #         duplicate_rows.to_excel(writer, sheet_name="all_duplicates", index=False)
# #         unique_rows.to_excel(writer, sheet_name="uniques", index=False)

# #     total_time = time.time() - start_time
# #     print(f"Cross-system output saved to: {output_path}")
# #     print(f"Final cross-system results: {len(final_rows)} total rows")
# #     print(f"⚡ Cross-system processing completed in {total_time:.2f} seconds")
# #     return output_path


# import pandas as pd
# import os
# import random
# from itertools import combinations
# from tqdm import tqdm
# from functools import partial


# # RapidFuzz Library Optimization
# try:
#     from rapidfuzz import fuzz
#     RAPIDFUZZ_AVAILABLE = True
#     print("✅ Using rapidfuzz for 5-10x faster fuzzy matching")
# except ImportError:
#     from fuzzywuzzy import fuzz
#     RAPIDFUZZ_AVAILABLE = False
#     print("⚠️ Install rapidfuzz for better performance: pip install rapidfuzz")
# print("your_existing_script.py loaded")

# import pandas as pd
# import re
# from itertools import combinations
# from collections import defaultdict
# #import jellyfish


# def generate_pair_chunks(idx_list, chunk_size=200_000):
#     n = len(idx_list)
#     batch = []
#     count = 0
#     for i_pos in range(n-1):
#         a = idx_list[i_pos]
#         for j_pos in range(i_pos+1, n):
#             b = idx_list[j_pos]
#             batch.append((a, b))
#             count += 1
#             if count >= chunk_size:
#                 yield batch
#                 batch = []
#                 count = 0
#     if batch:
#         yield batch

# def process_chunk_worker(chunk_pairs, df_values, fuzzy_columns, fuzzy_thresholds):
#     """
#     Worker for parallel all-pairs fuzzy comparison.
#     df_values: dict of column -> list/array of normalized strings (indexed by df.index)
#     """
#     res = []
#     for a, b in chunk_pairs:
#         match_scores = {}
#         passed = True
#         for col in fuzzy_columns:
#             threshold = fuzzy_thresholds.get(col, 90)
#             val_a, val_b = str(df_values[col][a]), str(df_values[col][b])
#             score = fast_fuzzy_ratio(val_a, val_b, threshold)
#             match_scores[col] = score
#             if score < threshold:
#                 passed = False
#                 break
#         if passed and all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
#             overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0
#             res.append((a, b, overall_score, match_scores))
#     return res


# def normalize_string(s: str) -> str:
#     if not s:
#         return ""
#     s = s.lower().strip()
#     s = re.sub(r'\s+', ' ', s)
#     return s


# def token_sort_block_key(s: str) -> str:
#     tokens = re.split(r'\s+', s)
#     tokens.sort()
#     return ' '.join(tokens)

# def phonetic_block_keys(s: str) -> list:
#     tokens = re.split(r'\s+', s)
#     return [jellyfish.metaphone(token) for token in tokens]

# def ngram_block_keys(s: str, n: int = 2) -> list:
#     s = re.sub(r'\s+', '', s)
#     return [s[i:i+n] for i in range(len(s)-n+1)] if len(s) >= n else [s]


# def generate_fuzzy_blocks(df, column):
#     blocks = defaultdict(list)
#     for idx, val in df[column].items():
#         val = normalize_string(str(val))
#         if not val:
#             continue
#         # Token-Sort
#         blocks[f"TOKEN_{token_sort_block_key(val)}"].append(idx)
#         # Phonetic
#         for p in phonetic_block_keys(val):
#             blocks[f"PHON_{p}"].append(idx)
#         # N-Grams
#         for n in ngram_block_keys(val, n=2):
#             blocks[f"NGRAM_{n}"].append(idx)
#     return blocks

# def generate_exact_blocks(df, exact_columns):
#     from collections import defaultdict
#     blocks = defaultdict(list)
#     for idx, row in df.iterrows():
#         key = tuple(row[col] for col in exact_columns)
#         blocks[key].append(idx)
#     return blocks

# #Candidate pair generation with exact blocking + fuzzy matching within blocks


# def preprocess_data_for_speed(df, fuzzy_columns, exact_columns):
#     """
#     Data Preprocessing Optimization
#     Clean and standardize data for faster comparisons
#     """
#     print("🔧 Preprocessing data for faster matching...")
    
#     # Make a copy to avoid modifying original
#     df = df.copy()
    
#     # Get all columns that will be used for matching
#     all_matching_columns = list(set(fuzzy_columns + exact_columns))
    
#     # Clean and standardize data efficiently
#     for col in all_matching_columns:
#         if col in df.columns:
#             # Fill NaN values, convert to string, strip whitespace, and convert to uppercase
#             df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
    
#     # Pre-calculate string lengths for fuzzy columns (used in length-based filtering)
#     string_lengths = {}
#     for col in fuzzy_columns:
#         if col in df.columns:
#             string_lengths[col] = df[col].str.len()
    
#     print(f"✅ Preprocessed {len(all_matching_columns)} matching columns")
#     return df, string_lengths


# def length_based_prefilter(val1, val2, threshold=90):
#     """
#     Length-Based Pre-filtering Optimization
#     Quick check before expensive fuzzy matching
#     """
#     # Quick exact match check
#     if val1 == val2:
#         return True, 100
    
#     # Handle empty strings
#     if not val1 or not val2:
#         return (not val1 and not val2), (100 if (not val1 and not val2) else 0)
    
#     # Length-based pre-filtering
#     len1, len2 = len(val1), len(val2)
#     if len1 == 0 and len2 == 0:
#         return True, 100
#     if len1 == 0 or len2 == 0:
#         return False, 0
    
#     # Calculate length ratio
#     length_ratio = (min(len1, len2) / max(len1, len2)) * 100
    
#     # If length difference is too large, skip expensive fuzzy matching
#     # Use conservative threshold to avoid false negatives
#     if length_ratio < threshold - 25:
#         return False, 0
    
#     # Passed pre-filter, proceed with fuzzy matching
#     return True, None


# def fast_fuzzy_ratio(val1, val2, threshold=90):
#     """
#     Optimized fuzzy matching with pre-filtering
#     """
#     # Apply length-based pre-filtering first
#     should_proceed, quick_score = length_based_prefilter(val1, val2, threshold)
    
#     if not should_proceed:
#         return quick_score
#     if quick_score is not None:  # Exact match found
#         return quick_score
    
#     # Proceed with fuzzy matching using RapidFuzz or FuzzyWuzzy
#     return fuzz.ratio(val1, val2)


# def union_find_grouping(matches):
#     """
#     Union-Find for Grouping Optimization
#     Efficient group assignment using Union-Find data structure
#     """
#     print("🔗 Using Union-Find for efficient group assignment...")
    
#     # Union-Find data structure
#     parent = {}
    
#     def find(x):
#         """Find root of element x with path compression"""
#         if x not in parent:
#             parent[x] = x
#         if parent[x] != x:
#             parent[x] = find(parent[x])  # Path compression
#         return parent[x]
    
#     def union(x, y):
#         """Union two elements into the same group"""
#         px, py = find(x), find(y)
#         if px != py:
#             parent[px] = py
    
#     # Process all matches to build groups
#     for idx_a, idx_b, overall_score, match_scores in matches:
#         union(idx_a, idx_b)
    
#     # Create group mapping
#     group_mapping = {}
#     group_id = 1
    
#     # Assign group IDs
#     all_indices = set()
#     for idx_a, idx_b, _, _ in matches:
#         all_indices.add(idx_a)
#         all_indices.add(idx_b)
    
#     for idx in all_indices:
#         root = find(idx)
#         if root not in group_mapping:
#             group_mapping[root] = group_id
#             group_id += 1
    
#     # Return final group assignments
#     final_groups = {}
#     for idx in all_indices:
#         root = find(idx)
#         final_groups[idx] = group_mapping[root]
    
#     return final_groups, group_id

# def get_rule_matches(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90, rule_id=None):
#     """
#     Run a single rule and return matches as list of tuples
#     Each tuple: (idx_a, idx_b, overall_score, match_scores)
#     """
#     df, _ = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
#     all_matches = []

#     for a, b in combinations(df.index, 2):
#         match_scores = {}

#         # fuzzy matching
#         for column in fuzzy_columns:
#             threshold = fuzzy_thresholds.get(column, 90)
#             val_a, val_b = str(df.at[a, column]), str(df.at[b, column])
#             score = fast_fuzzy_ratio(val_a, val_b, threshold)
#             match_scores[column] = score
#             if score < threshold:
#                 break  # early exit

#         # check if all fuzzy passed
#         if all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
#             # check exact columns
#             exact_match = all(df.at[a, col] == df.at[b, col] for col in exact_columns)
#             overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0

#             if exact_match and overall_score >= exact_threshold:
#                 all_matches.append((a, b, overall_score, match_scores,rule_id))

#     return all_matches

# def find_fuzzy_duplicates_multi_rules(df, rules, exact_threshold=90):
#     """
#     Process multiple rules:
#       - Run get_rule_matches for each rule independently
#       - Combine all matches
#       - Apply Union-Find ONCE to handle transitivity
#     """
#     print(f"Processing {len(rules)} rules for duplicate detection...")

#     all_matches = []
#     for i, rule in enumerate(rules, start=1):
#         fuzzy_cols = rule.get("fuzzy_columns", [])
#         exact_cols = rule.get("exact_columns", [])
#         thresholds = rule.get("thresholds", {})
#         print(f"▶ Rule {i}: fuzzy={fuzzy_cols}, exact={exact_cols}, thresholds={thresholds}")

#         matches = get_rule_matches(df, fuzzy_cols, exact_cols, thresholds, exact_threshold,rule_id=f"Rule_{i}")
#         print(f"   Rule {i} matches found: {len(matches)}")
#         all_matches.extend(matches)

#     print(f"Total combined matches from all rules: {len(all_matches)}")

#     if "matched_rules" not in df.columns:
#         df["matched_rules"] = ""

#     # Assign group IDs with union-find
#     if all_matches:
#         matches_for_union_find = [(a, b, s, m) for (a, b, s, m, r) in all_matches]
#         groups, next_group_id = union_find_grouping(matches_for_union_find)


#         for idx_a, idx_b, overall_score, match_scores, rule_id in all_matches:
#             # Update match percentage
#             df.at[idx_a, "match_percentage"] = float(overall_score)
#             df.at[idx_b, "match_percentage"] = float(overall_score)


#             # Update per-column fuzzy percentages
#             for col, score in match_scores.items():
#                 col_name = f"{col}_fuzzy_match_percentage"
#                 if col_name not in df.columns:
#                     df[col_name] = 0.0
#                 df.at[idx_a, col_name] = score
#                 df.at[idx_b, col_name] = score

#             for idx in [idx_a, idx_b]:
#                 if pd.isnull(df.at[idx, "matched_rules"]) or df.at[idx, "matched_rules"] == "":
#                     df.at[idx, "matched_rules"] = rule_id
#                 else:
#                     existing = str(df.at[idx, "matched_rules"])
#                     if rule_id not in existing.split(","):
#                         df.at[idx, "matched_rules"] = existing + "," + rule_id

#         for index, group_id in groups.items():
#             df.at[index, "group_id"] = group_id

#         group_id = next_group_id
#     else:
#         group_id = 1

#     # Assign unique group IDs for unmatched rows
#     for index, row in df.iterrows():
#         if pd.isnull(row.get("group_id")):
#             df.at[index, "group_id"] = group_id
#             group_id += 1

#     duplicate_groups = len([g for g in df["group_id"].value_counts() if g > 1])
#     print(f"✅ Found {duplicate_groups} duplicate groups across all rules")

#     return df


# def find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90):
#     print(f"Finding duplicates with fuzzy_columns: {fuzzy_columns}, exact_columns: {exact_columns}")
    
#     # Data Preprocessing Optimization
#     df, string_lengths = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
    
#     # Initialize result columns
#     df['group_id'] = None
#     df['match_percentage'] = 0.0
#     for column in fuzzy_columns:
#         df[f'{column}_fuzzy_match_percentage'] = 0.0

#     for column in exact_columns:
#         df[f'Exact_{column}_Score'] = 0.0

#     # Store all matches for Union-Find processing
#     all_matches = []
#     # CASE 1: Only exact columns
#     if fuzzy_columns == [] and exact_columns != []:
#         exact_blocks = generate_exact_blocks(df, exact_columns)
#         for block_indices in exact_blocks.values():
#             if len(block_indices) < 2:
#                 continue
#             for a, b in combinations(block_indices, 2):
#                 overall_score = 100.0
#                 #all_matches.append((a, b, overall_score, {}, "Exact_Only"))
#                 all_matches.append((a, b, overall_score, {}))


#                 for col in exact_columns:
#                     df.at[a, f'Exact_{col}_Score'] = 100
#                     df.at[b, f'Exact_{col}_Score'] = 100


#     # CASE 2: Only fuzzy columns
#     # --- REPLACEMENT: Lossless, chunked, progress-aware all-pairs (plug into CASE 2) ---
#     elif fuzzy_columns != [] and exact_columns == []:
#         print("⚠️ Performing full all-pairs comparisons (lossless).")

#         indices = list(df.index)
#         n = len(indices)
#         total_pairs = n*(n-1)//2
#         print(f"Total all-pairs to evaluate: {total_pairs:,} (n={n})")

#         CHUNK_SIZE = 200_000
#         USE_PARALLEL = True

#         # Precompute normalized strings for each fuzzy column to avoid repeated str(df.at[...])
#         df_values = {col: df[col].to_dict() for col in fuzzy_columns}

#         if not USE_PARALLEL:
#             for chunk in tqdm(generate_pair_chunks(indices, CHUNK_SIZE), total=(total_pairs//CHUNK_SIZE)+1, desc="AllPairs"):
#                 all_matches.extend(process_chunk_worker(chunk, df_values, fuzzy_columns, fuzzy_thresholds))
#         else:
#             import multiprocessing as mp
#             cpu_count = max(1, mp.cpu_count() - 1)
#             print(f"Using multiprocessing with {cpu_count} workers")
#             pool = mp.Pool(cpu_count)
#             try:
#                 chunks = list(generate_pair_chunks(indices, CHUNK_SIZE))
#                 total_chunks = len(chunks)
#                 from functools import partial
#                 worker_fn = partial(process_chunk_worker, df_values=df_values, fuzzy_columns=fuzzy_columns, fuzzy_thresholds=fuzzy_thresholds)
#                 for chunk_result in tqdm(pool.imap_unordered(worker_fn, chunks), total=total_chunks, desc="ParallelAllPairs"):
#                     if chunk_result:
#                         all_matches.extend(chunk_result)
#             finally:
#                 pool.close()
#                 pool.join()

#         print(f"All-pairs evaluation complete. Candidate matches found: {len(all_matches)}")

#     # CASE 3: Both fuzzy + exact columns
#     elif fuzzy_columns != [] and exact_columns != []:
#         exact_blocks = generate_exact_blocks(df, exact_columns)
#         for block_indices in exact_blocks.values():
#             if len(block_indices) < 2:
#                 continue
#             # Generate fuzzy blocks inside exact block
#             for a, b in combinations(block_indices, 2):
#                 match_scores = {}
#                 for col in fuzzy_columns:
#                     threshold = fuzzy_thresholds.get(col, 90)
#                     val_a, val_b = str(df.at[a, col]), str(df.at[b, col])
#                     score = fast_fuzzy_ratio(val_a, val_b, threshold)
#                     match_scores[col] = score
#                     if score < threshold:
#                         break
#                 if all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
#                     overall_score = sum(match_scores.values()) / len(match_scores)
#                     all_matches.append((a, b, overall_score, match_scores))


#                     for col in exact_columns:
#                         if df.at[a, col] == df.at[b, col]:
#                             df.at[a, f'Exact_{col}_Score'] = 100
#                             df.at[b, f'Exact_{col}_Score'] = 100
#                         else:
#                             df.at[a, f'Exact_{col}_Score'] = 0
#                             df.at[b, f'Exact_{col}_Score'] = 0 
                            
#                             #exact column

#     print(f"Total candidate matches collected: {len(all_matches)}")


#     # Union-Find for Grouping Optimization
#     if all_matches:
#         groups, next_group_id = union_find_grouping(all_matches)
        
#         # Assign group IDs and match scores
#         for idx_a, idx_b, overall_score, match_scores in all_matches:
#             # Update match percentages
#             df.at[idx_a, 'match_percentage'] = float(overall_score)
#             df.at[idx_b, 'match_percentage'] = float(overall_score)
            
#             # Update individual fuzzy match percentages
#             for column in fuzzy_columns:
#                 if column in match_scores:
#                     df.at[idx_a, f'{column}_fuzzy_match_percentage'] = match_scores[column]
#                     df.at[idx_b, f'{column}_fuzzy_match_percentage'] = match_scores[column]
        
#         # Assign group IDs from Union-Find results
#         for index, group_id in groups.items():
#             df.at[index, 'group_id'] = group_id
        
#         group_id = next_group_id
#     else:
#         group_id = 1

#     # Assign unique group IDs to unmatched records
#     for index, row in df.iterrows():
#         if pd.isnull(row['group_id']):
#             df.at[index, 'group_id'] = group_id
#             group_id += 1

#     duplicate_groups = len([g for g in df['group_id'].value_counts() if g > 1])
#     print(f"Found {duplicate_groups} duplicate groups using optimized Union-Find")
#     return df

# def assign_winner(df, source_system, rulebook, is_cross_system=False, source_system_main_file=None):
#     print(f"Assigning winners for source_system: {source_system}, is_cross_system: {is_cross_system}")
    
#     # Handle different possible transaction date column names
#     transaction_date_col = None
#     possible_date_names = ['Transaction Date', 'Transaction_Date', 'transaction_date', 'TransactionDate', 'Date', 'date']
    
#     for col_name in possible_date_names:
#         if col_name in df.columns:
#             transaction_date_col = col_name
#             break
    
#     if transaction_date_col is None:
#         print("⚠️ No transaction date column found, using row index as fallback")
#         df['Transaction_Date_Fallback'] = pd.to_datetime('2023-01-01') + pd.to_timedelta(df.index, unit='D')
#         transaction_date_col = 'Transaction_Date_Fallback'
    
#     # Convert to datetime
#     df[transaction_date_col] = pd.to_datetime(df[transaction_date_col], errors='coerce')
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
#                     winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
#                 elif winning_criteria == 'earliest_transaction_date':
#                     winner_id = group.sort_values(by=transaction_date_col, ascending=True).iloc[0]['Cust_Id']
#                 elif winning_criteria == 'largest_name':
#                     # Handle different possible name column names
#                     name_col = None
#                     for col in ['first_name', 'First_Name', 'firstName', 'name']:
#                         if col in df.columns:
#                             name_col = col
#                             break
                    
#                     if name_col:
#                         winner_id = group.loc[group[name_col].str.len().idxmax()]['Cust_Id']
#                     else:
#                         print(f"⚠️ No name column found for 'largest_name' criteria, using latest date")
#                         winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
#                 else:
#                     winner_id = group.sort_values(by=transaction_date_col, ascending=False).iloc[0]['Cust_Id']
                
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
    
#     # Record start time for performance measurement
#     import time
#     start_time = time.time()
    
#     df = pd.read_excel(file_path)
#     df.columns = df.columns.str.strip()
#     original_columns = df.columns.tolist()
#     print(f"Original columns: {original_columns}")
#     print(f"Data shape: {df.shape}")

#     source_system = os.path.splitext(os.path.basename(file_path))[0]
#     source_system_rule = source_system.split('_')[0]
#     print(f"Source system: {source_system}, Rule system: {source_system_rule}")

#     # Apply optimized fuzzy duplicate detection
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

#     # Performance summary
#     total_time = time.time() - start_time
#     print(f"Output saved to: {output_path}")
#     print(f"⚡ Processing completed in {total_time:.2f} seconds")
#     if total_time > 0:
#         print(f"⚡ Processing rate: {len(df) / total_time:.0f} records/second")
    
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
    
#     import time
#     start_time = time.time()
    
#     df = pd.read_excel(combined_excel_file, sheet_name='crosssystem_input')
#     print(f"Cross-system input data shape: {df.shape}")
#     print(f"Source systems in data: {df['Source_System'].unique()}")
    
#     # Apply optimized fuzzy duplicate detection
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

#     total_time = time.time() - start_time
#     print(f"Cross-system output saved to: {output_path}")
#     print(f"Final cross-system results: {len(final_rows)} total rows")
#     print(f"⚡ Cross-system processing completed in {total_time:.2f} seconds")
#     return output_path

import pandas as pd
import os
import random
from itertools import combinations
from tqdm import tqdm
from functools import partial
from functools import lru_cache
import numpy as np
import multiprocessing as mp


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

import pandas as pd
import re
from itertools import combinations
from collections import defaultdict
#import jellyfish


def generate_pair_chunks(idx_list, chunk_size=200_000):
    n = len(idx_list)
    batch = []
    count = 0
    for i_pos in range(n-1):
        a = idx_list[i_pos]
        for j_pos in range(i_pos+1, n):
            b = idx_list[j_pos]
            batch.append((a, b))
            count += 1
            if count >= chunk_size:
                yield batch
                batch = []
                count = 0
    if batch:
        yield batch

# def process_chunk_worker(chunk_pairs, df_values, fuzzy_columns, fuzzy_thresholds):
#     """
#     Worker for parallel all-pairs fuzzy comparison.
#     df_values: dict of column -> list/array of normalized strings (indexed by df.index)
#     """
#     res = []
#     for a, b in chunk_pairs:
#         match_scores = {}
#         passed = True
#         for col in fuzzy_columns:
#             threshold = fuzzy_thresholds.get(col, 90)
#             val_a, val_b = str(df_values[col][a]), str(df_values[col][b])
#             score = fast_fuzzy_ratio(val_a, val_b, threshold)
#             match_scores[col] = score
#             if score < threshold:
#                 passed = False
#                 break
#         if passed and all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
#             overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0
#             res.append((a, b, overall_score, match_scores))
#     return res


def process_chunk_worker(chunk_pairs, values_lists, fuzzy_columns, fuzzy_thresholds):
    """
    chunk_pairs : list of (pos_i, pos_j) where pos is 0..n-1 (position in lists)
    values_lists : dict col -> list(strings) indexed by position
    fuzzy_columns : list of fuzzy columns
    fuzzy_thresholds : dict col->int threshold
    """
    res = []
    # localize for speed
    get_thresh = fuzzy_thresholds.get
    cols = fuzzy_columns
    vals = values_lists
    score_fn = fast_fuzzy_ratio

    for pos_i, pos_j in chunk_pairs:
        match_scores = {}
        passed = True
        for col in cols:
            threshold = get_thresh(col, 90)
            val_a = vals[col][pos_i]
            val_b = vals[col][pos_j]
            score = score_fn(val_a, val_b, threshold)
            match_scores[col] = score
            if score < threshold:
                passed = False
                break
        if passed:
            overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0
            res.append((pos_i, pos_j, overall_score, match_scores))
    return res


def generate_pos_pair_chunks(n, chunk_size=200_000):
    """
    Generate batches of (pos_i, pos_j) pairs using positions 0..n-1.
    Streaming: yields lists of pairs up to chunk_size.
    """
    batch = []
    count = 0
    for i in range(n-1):
        for j in range(i+1, n):
            batch.append((i, j))
            count += 1
            if count >= chunk_size:
                yield batch
                batch = []
                count = 0
    if batch:
        yield batch

def find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90, use_parallel=True, chunk_size=200_000):
    """
    Drop-in optimized replacement for your find_fuzzy_duplicates:
      - Uses vectorized preprocessing
      - Uses LRU cached fuzzy scores
      - Streams pair chunks to multiprocessing pool (Windows-safe if run as script)
      - Returns DataFrame with same outputs: group_id, match_percentage, per-column fuzzy % etc.
    """
    print(f"Finding duplicates (optimized) with fuzzy_columns={fuzzy_columns}, exact_columns={exact_columns}")

    # 1) Preprocess (vectorized)
    df, values_lists, lengths, idx_to_pos, pos_to_idx, exact_values = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)

    n = len(df)
    # initialize result columns
    df['group_id'] = None
    df['match_percentage'] = 0.0
    for col in fuzzy_columns:
        df[f'{col}_fuzzy_match_percentage'] = 0.0
    for col in exact_columns:
        df[f'Exact_{col}_Score'] = 0.0

    all_matches = []

    # CASE 1: exact-only (same as before)
    if(not fuzzy_columns) and exact_columns:
        exact_blocks = generate_exact_blocks(df, exact_columns)
        for block in exact_blocks.values():
            if len(block) < 2:
                continue
            for a_idx, b_idx in combinations(block, 2):
                # build per-column exact match scores so later assignment is uniform
                match_scores = {}
                for col in exact_columns:
                    # both values should already be normalized by preprocess_data_for_speed
                    if df.at[a_idx, col] == df.at[b_idx, col]:
                        match_scores[col] = 100.0
                    else:
                        match_scores[col] = 0.0
                    # also set the explicit Exact_<col>_Score columns now
                    df.at[a_idx, f'Exact_{col}_Score'] = match_scores[col]
                    df.at[b_idx, f'Exact_{col}_Score'] = match_scores[col]

                # append a candidate match in the same (idx_a, idx_b, overall_score, match_scores) format
                all_matches.append((a_idx, b_idx, 100.0, match_scores))

    # CASE 2: fuzzy-only -> all-pairs streaming + multiprocessing on position pairs
    elif fuzzy_columns and (not exact_columns):
        total_pairs = n * (n - 1) // 2
        print(f"Total all-pairs to evaluate: {total_pairs:,} (n={n})")

        # create values_lists indexed by position already (we have that)
        # Use multiprocessing pool if requested
        if use_parallel and n > 2000:
            cpu_count = max(1, mp.cpu_count() - 1)
            print(f"Using multiprocessing with {cpu_count} workers")
            pool = mp.Pool(cpu_count)
            try:
                gen = generate_pos_pair_chunks(n, chunk_size=chunk_size)
                worker_fn = partial(process_chunk_worker, values_lists=values_lists, fuzzy_columns=fuzzy_columns, fuzzy_thresholds=fuzzy_thresholds)
                # stream to pool.imap_unordered
                for chunk_result in tqdm(pool.imap_unordered(worker_fn, gen), total=(total_pairs // chunk_size) + 1, desc="ParallelAllPairs"):
                    if chunk_result:
                        all_matches.extend([(pos_to_idx[a], pos_to_idx[b], s, m) for (a, b, s, m) in chunk_result])
            finally:
                pool.close()
                pool.join()
        else:
            # single-process streaming
            for batch in tqdm(generate_pos_pair_chunks(n, chunk_size=chunk_size), desc="AllPairs"):
                res = process_chunk_worker(batch, values_lists, fuzzy_columns, fuzzy_thresholds)
                all_matches.extend([(pos_to_idx[a], pos_to_idx[b], s, m) for (a, b, s, m) in res])

        print(f"All-pairs evaluation complete. Candidate matches found: {len(all_matches)}")

    # CASE 3: fuzzy + exact -> exact blocks + fuzzy inside blocks (as before, but using cached scorer)
    elif fuzzy_columns and exact_columns:
        exact_blocks = generate_exact_blocks(df, exact_columns)
        for block in exact_blocks.values():
            if len(block) < 2:
                continue
            # convert block indices to positions for faster access
            block_positions = [idx_to_pos[idx] for idx in block]
            for i_pos, j_pos in combinations(block_positions, 2):
                match_scores = {}
                passed = True
                for col in fuzzy_columns:
                    threshold = fuzzy_thresholds.get(col, 90)
                    val_a = values_lists[col][i_pos]
                    val_b = values_lists[col][j_pos]
                    score = fast_fuzzy_ratio(val_a, val_b, threshold)
                    match_scores[col] = score
                    if score < threshold:
                        passed = False
                        break
                if passed:
                    overall_score = sum(match_scores.values()) / len(match_scores) if match_scores else 0.0
                    all_matches.append((pos_to_idx[i_pos], pos_to_idx[j_pos], overall_score, match_scores))
                    # exact columns scores
                    for col in exact_columns:
                        if df.at[pos_to_idx[i_pos], col] == df.at[pos_to_idx[j_pos], col]:
                            df.at[pos_to_idx[i_pos], f'Exact_{col}_Score'] = 100
                            df.at[pos_to_idx[j_pos], f'Exact_{col}_Score'] = 100
                        else:
                            df.at[pos_to_idx[i_pos], f'Exact_{col}_Score'] = 0
                            df.at[pos_to_idx[j_pos], f'Exact_{col}_Score'] = 0

    # final grouping + assignment (same as before)
    print(f"Total candidate matches collected: {len(all_matches)}")

    if all_matches:
        groups, next_group_id = union_find_grouping(all_matches)
        # assign match_percentage and per-col fuzzy percentages
        for idx_a, idx_b, overall_score, match_scores in all_matches:
            df.at[idx_a, 'match_percentage'] = float(overall_score)
            df.at[idx_b, 'match_percentage'] = float(overall_score)
            for col, score in match_scores.items():
                df.at[idx_a, f'{col}_fuzzy_match_percentage'] = score
                df.at[idx_b, f'{col}_fuzzy_match_percentage'] = score

        for idx, gid in groups.items():
            df.at[idx, 'group_id'] = gid
        group_id = next_group_id
    else:
        group_id = 1

    # assign unique ids to unmatched
    for idx in df.index:
        if pd.isnull(df.at[idx, 'group_id']):
            df.at[idx, 'group_id'] = group_id
            group_id += 1

    duplicate_groups = len([g for g in df['group_id'].value_counts() if g > 1])
    print(f"Found {duplicate_groups} duplicate groups using optimized pipeline")
    return df
def normalize_string(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r'\s+', ' ', s)
    return s


def token_sort_block_key(s: str) -> str:
    tokens = re.split(r'\s+', s)
    tokens.sort()
    return ' '.join(tokens)

def phonetic_block_keys(s: str) -> list:
    tokens = re.split(r'\s+', s)
    return [jellyfish.metaphone(token) for token in tokens]

def ngram_block_keys(s: str, n: int = 2) -> list:
    s = re.sub(r'\s+', '', s)
    return [s[i:i+n] for i in range(len(s)-n+1)] if len(s) >= n else [s]


def generate_fuzzy_blocks(df, column):
    blocks = defaultdict(list)
    for idx, val in df[column].items():
        val = normalize_string(str(val))
        if not val:
            continue
        # Token-Sort
        blocks[f"TOKEN_{token_sort_block_key(val)}"].append(idx)
        # Phonetic
        for p in phonetic_block_keys(val):
            blocks[f"PHON_{p}"].append(idx)
        # N-Grams
        for n in ngram_block_keys(val, n=2):
            blocks[f"NGRAM_{n}"].append(idx)
    return blocks

def generate_exact_blocks(df, exact_columns):
    from collections import defaultdict
    blocks = defaultdict(list)
    for idx, row in df.iterrows():
        key = tuple(row[col] for col in exact_columns)
        blocks[key].append(idx)
    return blocks

#Candidate pair generation with exact blocking + fuzzy matching within blocks


# def preprocess_data_for_speed(df, fuzzy_columns, exact_columns):
#     """
#     Data Preprocessing Optimization
#     Clean and standardize data for faster comparisons
#     """
#     print("🔧 Preprocessing data for faster matching...")
    
#     # Make a copy to avoid modifying original
#     df = df.copy()
    
#     # Get all columns that will be used for matching
#     all_matching_columns = list(set(fuzzy_columns + exact_columns))
    
#     # Clean and standardize data efficiently
#     for col in all_matching_columns:
#         if col in df.columns:
#             # Fill NaN values, convert to string, strip whitespace, and convert to uppercase
#             df[col] = df[col].fillna('').astype(str).str.strip().str.upper()
    
#     # Pre-calculate string lengths for fuzzy columns (used in length-based filtering)
#     string_lengths = {}
#     for col in fuzzy_columns:
#         if col in df.columns:
#             string_lengths[col] = df[col].str.len()
    
#     print(f"✅ Preprocessed {len(all_matching_columns)} matching columns")
#     return df, string_lengths

def preprocess_data_for_speed(df, fuzzy_columns, exact_columns, normalize_to_upper=True):
    """
    Vectorized preprocessing (lossless):
      - fills NaN, converts to str, strips, collapses multiple spaces
      - returns:
          df (cleaned copy),
          values_lists: dict col -> list ordered by df.index (fast to share with workers)
          lengths: dict col -> numpy int32 array of lengths (ordered by df.index)
          idx_to_pos: dict original index -> position (0..n-1)
          pos_to_idx: list position -> original index
          exact_values: dict col -> list for exact comparisons
    """
    df = df.copy()
    pos_to_idx = list(df.index)
    idx_to_pos = {idx: pos for pos, idx in enumerate(pos_to_idx)}
    all_matching_columns = list(dict.fromkeys(list(fuzzy_columns) + list(exact_columns)))

    for col in all_matching_columns:
        if col not in df.columns:
            df[col] = ''
        s = df[col].fillna('').astype(str)
        s = s.str.strip().str.replace(r'\s+', ' ', regex=True)
        if normalize_to_upper:
            s = s.str.upper()
        df[col] = s

    n = len(df)
    values_lists = {}
    lengths = {}
    for col in fuzzy_columns:
        col_list = df[col].to_list()
        values_lists[col] = col_list
        lengths[col] = np.fromiter((len(x) for x in col_list), dtype=np.int32, count=n)

    exact_values = {col: df[col].to_list() for col in exact_columns}

    return df, values_lists, lengths, idx_to_pos, pos_to_idx, exact_values


# def length_based_prefilter(val1, val2, threshold=90):
#     """
#     Length-Based Pre-filtering Optimization
#     Quick check before expensive fuzzy matching
#     """
#     # Quick exact match check
#     if val1 == val2:
#         return True, 100
    
#     # Handle empty strings
#     if not val1 or not val2:
#         return (not val1 and not val2), (100 if (not val1 and not val2) else 0)
    
#     # Length-based pre-filtering
#     len1, len2 = len(val1), len(val2)
#     if len1 == 0 and len2 == 0:
#         return True, 100
#     if len1 == 0 or len2 == 0:
#         return False, 0
    
#     # Calculate length ratio
#     length_ratio = (min(len1, len2) / max(len1, len2)) * 100
    
#     # If length difference is too large, skip expensive fuzzy matching
#     # Use conservative threshold to avoid false negatives
#     if length_ratio < threshold - 25:
#         return False, 0
    
#     # Passed pre-filter, proceed with fuzzy matching
#     return True, None

def length_based_prefilter(val1, val2, threshold=90):
    """
    Lossless length-based prefilter logic (same behavior as your original).
    Returns: (should_proceed_bool, quick_score_or_None)
    """
    # Quick exact match
    if val1 == val2:
        return True, 100

    # Handle empty strings
    if not val1 or not val2:
        return (not val1 and not val2), (100 if (not val1 and not val2) else 0)

    len1, len2 = len(val1), len(val2)
    if len1 == 0 and len2 == 0:
        return True, 100
    if len1 == 0 or len2 == 0:
        return False, 0

    length_ratio = (min(len1, len2) / max(len1, len2)) * 100
    if length_ratio < threshold - 25:
        return False, 0

    return True, None

@lru_cache(maxsize=200_000)
def fast_fuzzy_ratio(a, b, threshold=90):
    """
    Wrapper that applies length prefilter then uses RapidFuzz fuzz.ratio.
    Strings must be normalized before caching (we pass already-cleaned strings).
    """
    should_proceed, quick_score = length_based_prefilter(a, b, threshold)
    if not should_proceed:
        return quick_score
    if quick_score is not None:
        return quick_score
    return fuzz.ratio(a, b)



# def fast_fuzzy_ratio(val1, val2, threshold=90):
#     """
#     Optimized fuzzy matching with pre-filtering
#     """
#     # Apply length-based pre-filtering first
#     should_proceed, quick_score = length_based_prefilter(val1, val2, threshold)
    
#     if not should_proceed:
#         return quick_score
#     if quick_score is not None:  # Exact match found
#         return quick_score
    
#     # Proceed with fuzzy matching using RapidFuzz or FuzzyWuzzy
#     return fuzz.ratio(val1, val2)


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


# def find_fuzzy_duplicates(df, fuzzy_columns, exact_columns, fuzzy_thresholds, exact_threshold=90):
#     print(f"Finding duplicates with fuzzy_columns: {fuzzy_columns}, exact_columns: {exact_columns}")
    
#     # Data Preprocessing Optimization
#     df, string_lengths = preprocess_data_for_speed(df, fuzzy_columns, exact_columns)
    
#     # Initialize result columns
#     df['group_id'] = None
#     df['match_percentage'] = 0.0
#     for column in fuzzy_columns:
#         df[f'{column}_fuzzy_match_percentage'] = 0.0

#     for column in exact_columns:
#         df[f'Exact_{column}_Score'] = 0.0

#     # Store all matches for Union-Find processing
#     all_matches = []
#     # CASE 1: Only exact columns
#     if fuzzy_columns == [] and exact_columns != []:
#         exact_blocks = generate_exact_blocks(df, exact_columns)
#         for block_indices in exact_blocks.values():
#             if len(block_indices) < 2:
#                 continue
#             for a, b in combinations(block_indices, 2):
#                 overall_score = 100.0
#                 #all_matches.append((a, b, overall_score, {}, "Exact_Only"))
#                 all_matches.append((a, b, overall_score, {}))


#                 for col in exact_columns:
#                     df.at[a, f'Exact_{col}_Score'] = 100
#                     df.at[b, f'Exact_{col}_Score'] = 100


#     # CASE 2: Only fuzzy columns
#     # --- REPLACEMENT: Lossless, chunked, progress-aware all-pairs (plug into CASE 2) ---
#     elif fuzzy_columns != [] and exact_columns == []:
#         print("⚠️ Performing full all-pairs comparisons (lossless).")

#         indices = list(df.index)
#         n = len(indices)
#         total_pairs = n*(n-1)//2
#         print(f"Total all-pairs to evaluate: {total_pairs:,} (n={n})")

#         CHUNK_SIZE = 200_000
#         USE_PARALLEL = True

#         # Precompute normalized strings for each fuzzy column to avoid repeated str(df.at[...])
#         df_values = {col: df[col].to_dict() for col in fuzzy_columns}

#         if not USE_PARALLEL:
#             for chunk in tqdm(generate_pair_chunks(indices, CHUNK_SIZE), total=(total_pairs//CHUNK_SIZE)+1, desc="AllPairs"):
#                 all_matches.extend(process_chunk_worker(chunk, df_values, fuzzy_columns, fuzzy_thresholds))
#         else:
#             import multiprocessing as mp
#             cpu_count = max(1, mp.cpu_count() - 1)
#             print(f"Using multiprocessing with {cpu_count} workers")
#             pool = mp.Pool(cpu_count)
#             try:
#                 chunks = list(generate_pair_chunks(indices, CHUNK_SIZE))
#                 total_chunks = len(chunks)
#                 from functools import partial
#                 worker_fn = partial(process_chunk_worker, df_values=df_values, fuzzy_columns=fuzzy_columns, fuzzy_thresholds=fuzzy_thresholds)
#                 for chunk_result in tqdm(pool.imap_unordered(worker_fn, chunks), total=total_chunks, desc="ParallelAllPairs"):
#                     if chunk_result:
#                         all_matches.extend(chunk_result)
#             finally:
#                 pool.close()
#                 pool.join()

#         print(f"All-pairs evaluation complete. Candidate matches found: {len(all_matches)}")

#     # CASE 3: Both fuzzy + exact columns
#     elif fuzzy_columns != [] and exact_columns != []:
#         exact_blocks = generate_exact_blocks(df, exact_columns)
#         for block_indices in exact_blocks.values():
#             if len(block_indices) < 2:
#                 continue
#             # Generate fuzzy blocks inside exact block
#             for a, b in combinations(block_indices, 2):
#                 match_scores = {}
#                 for col in fuzzy_columns:
#                     threshold = fuzzy_thresholds.get(col, 90)
#                     val_a, val_b = str(df.at[a, col]), str(df.at[b, col])
#                     score = fast_fuzzy_ratio(val_a, val_b, threshold)
#                     match_scores[col] = score
#                     if score < threshold:
#                         break
#                 if all(match_scores[c] >= fuzzy_thresholds.get(c, 90) for c in fuzzy_columns):
#                     overall_score = sum(match_scores.values()) / len(match_scores)
#                     all_matches.append((a, b, overall_score, match_scores))


#                     for col in exact_columns:
#                         if df.at[a, col] == df.at[b, col]:
#                             df.at[a, f'Exact_{col}_Score'] = 100
#                             df.at[b, f'Exact_{col}_Score'] = 100
#                         else:
#                             df.at[a, f'Exact_{col}_Score'] = 0
#                             df.at[b, f'Exact_{col}_Score'] = 0 
                            
#                             #exact column

#     print(f"Total candidate matches collected: {len(all_matches)}")


#     # Union-Find for Grouping Optimization
#     if all_matches:
#         groups, next_group_id = union_find_grouping(all_matches)
        
#         # Assign group IDs and match scores
#         for idx_a, idx_b, overall_score, match_scores in all_matches:
#             # Update match percentages
#             df.at[idx_a, 'match_percentage'] = float(overall_score)
#             df.at[idx_b, 'match_percentage'] = float(overall_score)
            
#             # Update individual fuzzy match percentages
#             for column in fuzzy_columns:
#                 if column in match_scores:
#                     df.at[idx_a, f'{column}_fuzzy_match_percentage'] = match_scores[column]
#                     df.at[idx_b, f'{column}_fuzzy_match_percentage'] = match_scores[column]
        
#         # Assign group IDs from Union-Find results
#         for index, group_id in groups.items():
#             df.at[index, 'group_id'] = group_id
        
#         group_id = next_group_id
#     else:
#         group_id = 1

#     # Assign unique group IDs to unmatched records
#     for index, row in df.iterrows():
#         if pd.isnull(row['group_id']):
#             df.at[index, 'group_id'] = group_id
#             group_id += 1

#     duplicate_groups = len([g for g in df['group_id'].value_counts() if g > 1])
#     print(f"Found {duplicate_groups} duplicate groups using optimized Union-Find")
#     return df

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