import json
import os
import re

# --- Configuration ---
# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIRECTORY_NAME = "Resultados JSON Unificados"
OUTPUT_BASE_DIRECTORY_NAME = "Outputs Poblacion SQLs"

INPUT_DIRECTORY = os.path.join(SCRIPT_DIR, INPUT_DIRECTORY_NAME)
OUTPUT_BASE_DIR = os.path.join(SCRIPT_DIR, OUTPUT_BASE_DIRECTORY_NAME)

SCHEMA_NAME = "public"
PRODUCTS_PER_SQL_FILE = 30 # Number of products per batched SQL file

# --- Dynamic Input File Selection ---
def select_input_file():
    """Lists JSON files in INPUT_DIRECTORY and prompts user for selection."""
    if not os.path.isdir(INPUT_DIRECTORY):
        print(f"Error: Input directory not found: {INPUT_DIRECTORY}")
        print(f"Please ensure the directory '{INPUT_DIRECTORY_NAME}' exists in the same folder as the script.")
        exit(1)

    json_files = [f for f in os.listdir(INPUT_DIRECTORY) if f.endswith('.json') and os.path.isfile(os.path.join(INPUT_DIRECTORY, f))]

    if not json_files:
        print(f"No JSON files found in directory: {INPUT_DIRECTORY}")
        exit(1)

    print("\nAvailable JSON files for processing:")
    for i, filename in enumerate(json_files):
        print(f"  {i + 1}. {filename}")

    selected_index = -1
    while True:
        try:
            choice = input(f"Enter the number of the JSON file to process (1-{len(json_files)}): ")
            selected_index = int(choice) - 1
            if 0 <= selected_index < len(json_files):
                break
            else:
                print(f"Invalid choice. Please enter a number between 1 and {len(json_files)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    selected_filename = json_files[selected_index]
    input_filepath = os.path.join(INPUT_DIRECTORY, selected_filename)
    print(f"\nSelected file for processing: {input_filepath}")
    return input_filepath, selected_filename

INPUT_FILENAME, SELECTED_JSON_FILENAME = select_input_file()
BASE_INPUT_NAME = os.path.splitext(SELECTED_JSON_FILENAME)[0] # For naming output files

# Ensure output directory exists
if not os.path.exists(OUTPUT_BASE_DIR):
    try:
        os.makedirs(OUTPUT_BASE_DIR)
        print(f"Created output directory: {OUTPUT_BASE_DIR}")
    except OSError as e:
        print(f"Error creating output directory {OUTPUT_BASE_DIR}: {e}")
        exit(1)


# --- Helper Functions ---

def safe_get(data, keys, default=None):
    """Safely accesses nested dictionary/list keys."""
    if not isinstance(keys, list):
        keys = [keys]
    current_data = data
    for key in keys:
        if isinstance(current_data, dict) and key in current_data:
            current_data = current_data[key]
        elif isinstance(current_data, list) and isinstance(key, int) and len(current_data) > key >= 0:
             current_data = current_data[key]
        else:
            return default
    return current_data

def clean_price(price_str):
    """Converts price string (e.g., '$3.450' or '2 x $2.500' or '0') to a float or None."""
    if price_str is None:
        return None
    price_str = str(price_str).strip()
    if not price_str: return None
    try: return float(price_str)
    except (ValueError, TypeError): pass
    match_dollar = re.match(r'^\$\s*(\d{1,3}(?:\.\d{3})*(?:,\d+)?)$', price_str)
    if match_dollar:
        numeric_part = match_dollar.group(1).replace('.', '').replace(',', '.')
        try: return float(numeric_part)
        except (ValueError, TypeError): return None
    match_promo = re.search(r'x\s*\$\s*(\d{1,3}(?:\.\d{3})*(?:,\d+)?)$', price_str)
    if match_promo:
         numeric_part = match_promo.group(1).replace('.', '').replace(',', '.')
         try: return float(numeric_part)
         except (ValueError, TypeError): pass
    return None

def escape_sql_string(value):
    if value is None: return None
    return str(value).replace("'", "''")

def boolean_to_sql(value):
    if value is True: return 'TRUE'
    elif value is False: return 'FALSE'
    else: return 'NULL'

def collect_unique_nutri_types_flat(nodes):
    flat_types = set()
    if not isinstance(nodes, list): return flat_types
    for node in nodes:
        name = safe_get(node, ['name'])
        unit = safe_get(node, ['energyUnit'])
        if name: flat_types.add((name, unit))
        children = safe_get(node, ['children'], [])
        if children: flat_types.update(collect_unique_nutri_types_flat(children))
    return flat_types

def flatten_nutri_nodes(nodes):
    flat_list = []
    if not isinstance(nodes, list): return flat_list
    for node in nodes:
        name = safe_get(node, ['name'])
        unit = safe_get(node, ['energyUnit'])
        value_100g = safe_get(node, ['energyValue'])
        value_portion = safe_get(node, ['energyValuePortion'])
        if name:
            flat_list.append({'name': name, 'unit': unit, 'value_100g': value_100g, 'value_portion': value_portion})
        children = safe_get(node, ['children'], [])
        if children: flat_list.extend(flatten_nutri_nodes(children))
    return flat_list

# --- Data Collection Structures ---
unique_brands = set()
# Change categories structure to map cat_id -> {name, slug, item_path_name} candidates
category_candidates_by_id = {}
unique_ingredients = set()
unique_nutri_type_names_units_all = set()
unique_cert_types = set()
# Change certifiers structure to map certifier_name -> {json_id, logo_url} candidates
certifier_candidates_by_name = {}
unique_cert_degrees = set()
unique_countries = set()
product_raw_data_list = []

# --- Main Processing (Collection Pass) ---
print(f"\nStarting data collection from {INPUT_FILENAME}...")
try:
    with open(INPUT_FILENAME, 'r', encoding='utf-8') as f:
        combined_json_data = json.load(f)
    product_data_dict = safe_get(combined_json_data, ['datos'], {})
    if not product_data_dict:
        print(f"Error: Could not find 'datos' key or it's empty in {INPUT_FILENAME}.")
        exit(1)

    print(f"Found {len(product_data_dict)} potential product entries in the 'datos' section.")
    processed_count = 0
    for filename_key, full_response in product_data_dict.items():
        page_props = safe_get(full_response, ['props', 'pageProps'])
        if not page_props: continue
        product_array = safe_get(page_props, ['product', 'products'])
        if not isinstance(product_array, list) or len(product_array) == 0: continue
        product_item_data = product_array[0]
        item_data = safe_get(product_item_data, ['item'])
        if not item_data: continue
        ean = safe_get(item_data, ['ean'])
        if not ean: continue

        processed_count += 1
        if processed_count % 100 == 0: print(f"Processed {processed_count} entries...")

        ean_detail_data = None
        dehydrated_queries = safe_get(page_props, ['dehydratedState', 'queries'], [])
        for query in dehydrated_queries:
             query_key = safe_get(query, ['queryKey'])
             query_state = safe_get(query, ['state'])
             if isinstance(query_key, list) and safe_get(query_key, [0]) == 'getProductDetailByEan' and safe_get(query_key, [1]) == str(ean):
                  if safe_get(query_state, ['status']) == 'success':
                       ean_detail_data_payload = safe_get(query_state, ['data', 'data'])
                       if ean_detail_data_payload:
                           ean_detail_data_response = safe_get(ean_detail_data_payload, ['response'])
                           if ean_detail_data_response: ean_detail_data = ean_detail_data_response
                  break

        brand_id = safe_get(item_data, ['brandId'])
        brand_name = safe_get(item_data, ['brand'])
        if brand_id is not None and brand_name and brand_name.strip():
             unique_brands.add((brand_id, brand_name.strip()))

        category_id = safe_get(item_data, ['categoryId'])
        category_slug = safe_get(item_data, ['categorySlug'])
        category_name_ean = safe_get(ean_detail_data, ['category_name']) if ean_detail_data else None
        category_name_item_path = None
        item_categories_path = safe_get(item_data, ['categories'], [])
        if isinstance(item_categories_path, list) and item_categories_path:
            path_segments = [seg.strip() for seg in item_categories_path[-1].split('/') if seg.strip()]
            if path_segments: category_name_item_path = path_segments[-1]

        if category_id is not None:
            if category_id not in category_candidates_by_id:
                 category_candidates_by_id[category_id] = {
                      'slug': category_slug,
                      'name_ean': category_name_ean,
                      'name_item_path': category_name_item_path
                 }
            else:
                 # Update with non-None values if found
                 current = category_candidates_by_id[category_id]
                 if category_slug is not None: current['slug'] = category_slug
                 if category_name_ean is not None: current['name_ean'] = category_name_ean
                 if category_name_item_path is not None: current['name_item_path'] = category_name_item_path


        if ean_detail_data:
            ingredients_sets = safe_get(ean_detail_data, ['ingredients_sets'], [])
            allergens_list_data = safe_get(ean_detail_data, ['allergens'], [])
            traces_list_data = safe_get(ean_detail_data, ['traces'], [])
            all_ing_like_items = []
            for ing_set in ingredients_sets: all_ing_like_items.extend(safe_get(ing_set, ['ingredients'], []))
            all_ing_like_items.extend(allergens_list_data)
            all_ing_like_items.extend(traces_list_data)
            for ing in all_ing_like_items:
                 ing_id = safe_get(ing, ['ingredient_id'])
                 ing_name = safe_get(ing, ['ingredient_name'])
                 if ing_name and ing_name.strip(): unique_ingredients.add((ing_id, ing_name.strip()))

            nutri_tables = safe_get(ean_detail_data, ['nutritional_tables_sets'])
            if nutri_tables:
                 nutri_info = safe_get(nutri_tables, ['nutritionalInfo'], [])
                 unique_nutri_type_names_units_all.update(collect_unique_nutri_types_flat(nutri_info))

            certificates = safe_get(ean_detail_data, ['certificates'], [])
            for cert in certificates:
                type_code = safe_get(cert, ['certification_type_code'])
                type_name = safe_get(cert, ['certification_type_name'])
                if type_code and type_code.strip() and type_name and type_name.strip():
                     unique_cert_types.add((type_code.strip(), type_name.strip()))
                certifiers_list = safe_get(cert, ['certifiers'], [])
                for certifier in certifiers_list:
                    certifier_json_id = safe_get(certifier, ['certifier_id'])
                    certifier_name = safe_get(certifier, ['certifier_name'])
                    certifier_logo = safe_get(certifier, ['certifier_logo_url'])
                    # Collect certifier candidates by name, potentially updating with non-None json_id/logo
                    if certifier_name and certifier_name.strip():
                         cleaned_name = certifier_name.strip()
                         if cleaned_name not in certifier_candidates_by_name:
                             certifier_candidates_by_name[cleaned_name] = {'json_id': certifier_json_id, 'logo_url': certifier_logo}
                         else:
                             # Prefer specific values over None
                             if certifier_json_id is not None and certifier_json_id != 0:
                                  certifier_candidates_by_name[cleaned_name]['json_id'] = certifier_json_id
                             if certifier_logo is not None:
                                  certifier_candidates_by_name[cleaned_name]['logo_url'] = certifier_logo
                    elif certifier_json_id is not None and certifier_json_id != 0:
                        # Handle certifiers with ID but no name - need a strategy or add to a different set
                        # For now, we only collect by name for the primary certifier upsert
                        pass # Could add logic here if needed for ID-only certifiers

                    degree_id = safe_get(certifier, ['certification_degree_id'])
                    degree_name = safe_get(certifier, ['certification_degree_name'])
                    if degree_id is not None and degree_name and degree_name.strip():
                        unique_cert_degrees.add((degree_id, degree_name.strip()))
                    country_id = safe_get(certifier, ['certification_country_id'])
                    country_name = safe_get(certifier, ['certification_country_name'])
                    if country_id is not None and country_name and country_name.strip():
                         unique_countries.add((country_id, country_name.strip()))
            origin_country_id = safe_get(ean_detail_data, ['origin_country_id'])
            origin_country_name = safe_get(ean_detail_data, ['origin_country_name'])
            if origin_country_id is not None and origin_country_name and origin_country_name.strip():
                 unique_countries.add((origin_country_id, origin_country_name.strip()))


        product_raw_data_list.append({
             'ean': str(ean), 'item': item_data,
             'price': safe_get(product_item_data, ['price']),
             'promotion': safe_get(product_item_data, ['promotion']),
             'ean_data': ean_detail_data
        })

    print(f"\nFinished data collection. Successfully processed {processed_count} valid product entries.")

except FileNotFoundError:
    print(f"Error: Input file not found at {INPUT_FILENAME}")
    exit(1)
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from {INPUT_FILENAME}. Please ensure it's a valid JSON object with a 'datos' key.")
    exit(1)
except Exception as e:
    print(f"An unexpected error occurred during data collection: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# --- SQL Generation Function ---
def write_sql_file(filename, statements_list):
    """Writes a list of SQL statements to a file, adding BEGIN/COMMIT."""
    filepath = os.path.join(OUTPUT_BASE_DIR, filename)
    print(f"Writing SQL statements to {filepath}...")
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("BEGIN;\n\n")
            for statement in statements_list:
                f.write(statement.strip().rstrip(';') + ";\n")
            f.write("\nCOMMIT;\n")
        print(f"Successfully generated SQL script: {filepath}")
    except IOError as e:
        print(f"Error writing SQL file {filepath}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during file writing for {filepath}: {e}")
        import traceback
        traceback.print_exc()

# --- Generate SQL for Lookup Tables ---
lookup_sql_statements = []
lookup_sql_statements.append("-- SQL script to populate Unimarc LOOKUP TABLES")
lookup_sql_statements.append(f"-- Generated by populate_sql.py from {SELECTED_JSON_FILENAME}")
lookup_sql_statements.append(f"-- Target Schema: {SCHEMA_NAME}")
lookup_sql_statements.append("-- Note: This script generates PostgreSQL syntax for ON CONFLICT (upsert).")
lookup_sql_statements.append("-- IMPORTANT: ON CONFLICT requires corresponding UNIQUE constraints or PRIMARY KEYs in the database schema.")
lookup_sql_statements.append("\n-- --- Lookup Tables ---")

# Brands
lookup_sql_statements.append("\n-- Brands (Requires UNIQUE(brand_id))")
for brand_id, brand_name in sorted(list(unique_brands)):
    lookup_sql_statements.append(f"INSERT INTO {SCHEMA_NAME}.brands_unimarc (brand_id, brand_name) VALUES ({brand_id}, '{escape_sql_string(brand_name)}') ON CONFLICT (brand_id) DO UPDATE SET brand_name = EXCLUDED.brand_name;")

# Categories - REVISED PRAGMATIC FIX relying on UNIQUE(category_id)
lookup_sql_statements.append("\n-- Categories (Requires UNIQUE(category_id). category_name will NOT be unique in the table if source data has name conflicts for different IDs.)")

# Derive the best name for each category_id found
category_details_final = {} # cat_id -> {"name": final_name, "slug": slug}

# Define a sort key that handles None for all string parts of the tuple for deterministic processing
# Although we are processing category_candidates_by_id which is a dict (no inherent order), sorting the keys makes the SQL output order deterministic.
sorted_category_ids = sorted(category_candidates_by_id.keys())


for cat_id in sorted_category_ids:
    candidates = category_candidates_by_id[cat_id]
    cat_name_ean = candidates['name_ean']
    cat_slug = candidates['slug']
    cat_name_item_path = candidates['name_item_path']

    final_cat_name = None
    # Prioritize EAN name if it looks reasonable
    if cat_name_ean and cat_name_ean.strip() and cat_name_ean.strip() not in ("Despensa", "Cóctel y snacks"):
        final_cat_name = cat_name_ean.strip()
    # Fallback to Item Path name if EAN name is missing or undesirable
    if final_cat_name is None and cat_name_item_path and cat_name_item_path.strip() and cat_name_item_path.strip() not in ("Despensa", "Cóctel y snacks", "Pastas frescas", "Aceitunas y encurtidos", ""):
        final_cat_name = cat_name_item_path.strip()
    # Fallback to derived name from slug
    if final_cat_name is None and cat_slug:
        last_slug_part = cat_slug.split('/')[-1]
        if last_slug_part and last_slug_part.strip() and last_slug_part.strip() not in ("despensa", "coctel-y-snacks", "pastas-frescas", "aceitunas-y-encurtidos", ""):
            final_cat_name = last_slug_part.replace('-', ' ').strip().title()

    # If we still don't have a name, maybe use one of the undesirable ones if no other choice?
    # Or, if category_name is NOT NULL, this ID might be skipped or need a placeholder.
    # Assuming category_name *can* be NULL or we prefer not to insert if no good name found.
    # Let's refine the check: only add to final_categories_final if we got *any* non-empty name.
    if final_cat_name and final_cat_name.strip():
         # Final check against the explicit exclusion list for the derived name
         if final_cat_name.strip() in ("Despensa", "Cóctel y snacks", "Pastas Frescas", "Aceitunas Y Encurtidos"):
             # Re-attempt derivation if the chosen name is excluded
             temp_name = None
             if cat_name_ean and cat_name_ean.strip() and cat_name_ean.strip() not in ("Despensa", "Cóctel y snacks"):
                 temp_name = cat_name_ean.strip()
             if temp_name is None and cat_name_item_path and cat_name_item_path.strip() and cat_name_item_path.strip() not in ("Despensa", "Cóctel y snacks", "Pastas frescas", "Aceitunas y encurtidos", ""):
                 temp_name = cat_name_item_path.strip()
             if temp_name is None and cat_slug:
                 last_slug_part = cat_slug.split('/')[-1]
                 if last_slug_part and last_slug_part.strip() and last_slug_part.strip() not in ("despensa", "coctel-y-snacks", "pastas-frescas", "aceitunas-y-encurtidos", ""):
                    temp_name = last_slug_part.replace('-', ' ').strip().title()

             if temp_name and temp_name.strip() and temp_name.strip() not in ("Despensa", "Cóctel y snacks", "Pastas Frescas", "Aceitunas Y Encurtidos"):
                  final_cat_name = temp_name.strip() # Use the fallback if the primary choice was excluded
             else:
                  # If all fallbacks are also excluded or empty, maybe set to None or log a warning
                  print(f"Warning: No suitable unique name derived for category ID {cat_id} after exclusions. Slug: {cat_slug}, EAN: {cat_name_ean}, Path: {cat_name_item_path}. This ID might be inserted with a potentially generic name or NULL if schema allows.")
                  # Keep the best *attempt* even if it's one of the excluded ones, or set to None?
                  # Let's keep the best attempt, the exclusion was likely for the unique name step we removed.
                  final_cat_name = (cat_name_ean or cat_name_item_path or (cat_slug.split('/')[-1].replace('-', ' ').strip().title() if cat_slug else None) or '').strip() or None # Re-derive without exclusions
                  if final_cat_name is None:
                       print(f"Warning: Category ID {cat_id} resulted in a NULL or empty name after derivation attempts. Skipping insertion for this ID.")
                       continue # Skip if no name could be derived at all.


         category_details_final[cat_id] = {"name": final_cat_name, "slug": cat_slug}
    else:
        # Log categories for which no name could be derived
        print(f"Warning: No name derived for category ID {cat_id}. Slug: {cat_slug}, EAN: {cat_name_ean}, Path: {cat_name_item_path}. Skipping insertion for this ID.")


# Generate SQL using ON CONFLICT (category_id) for unique IDs
for cat_id, details in category_details_final.items():
    derived_name = details["name"]
    derived_slug = details["slug"]
    cat_name_sql = f"'{escape_sql_string(derived_name)}'" if derived_name is not None else 'NULL'
    cat_slug_sql = f"'{escape_sql_string(derived_slug)}'" if derived_slug is not None else 'NULL'

    lookup_sql_statements.append(
        f"INSERT INTO {SCHEMA_NAME}.categories_unimarc (category_id, category_name, category_slug) "
        f"VALUES ({cat_id}, {cat_name_sql}, {cat_slug_sql}) "
        f"ON CONFLICT (category_id) DO UPDATE SET "
        f"category_name = EXCLUDED.category_name, "
        f"category_slug = EXCLUDED.category_slug;"
    )


# Ingredients
lookup_sql_statements.append("\n-- Ingredient/Allergen/Trace Names (Requires UNIQUE(ingredient_name))")
for json_id, name in sorted(list(unique_ingredients), key=lambda x: x[1]):
     if not name or not name.strip(): continue
     json_id_sql = json_id if json_id is not None else 'NULL' # Store raw JSON ID as number if available
     cleaned_name = name.strip()
     lookup_sql_statements.append(f"INSERT INTO {SCHEMA_NAME}.ingredients_unimarc (json_ingredient_id, ingredient_name) VALUES ({json_id_sql}, '{escape_sql_string(cleaned_name)}') ON CONFLICT (ingredient_name) DO UPDATE SET json_ingredient_id = COALESCE(EXCLUDED.json_ingredient_id, {SCHEMA_NAME}.ingredients_unimarc.json_ingredient_id);")

# Nutritional Info Types
lookup_sql_statements.append("\n-- Nutritional Info Types (Requires UNIQUE(name))")
nutri_type_sort_key = lambda x: (str(x[0]) if x[0] is not None else "", str(x[1]) if x[1] is not None else "")
for name, unit in sorted(list(unique_nutri_type_names_units_all), key=nutri_type_sort_key):
    if not name or not name.strip(): continue
    unit_sql = f"'{escape_sql_string(unit)}'" if unit is not None else 'NULL'
    cleaned_name = name.strip()
    lookup_sql_statements.append(f"INSERT INTO {SCHEMA_NAME}.nutritional_info_types_unimarc (name, unit) VALUES ('{escape_sql_string(cleaned_name)}', {unit_sql}) ON CONFLICT (name) DO UPDATE SET unit = EXCLUDED.unit;")

# Certification Types
lookup_sql_statements.append("\n-- Certification Types (Requires UNIQUE(certification_type_code))")
cert_type_sort_key = lambda x: (str(x[0]) if x[0] is not None else "", str(x[1]) if x[1] is not None else "")
for code, name in sorted(list(unique_cert_types), key=cert_type_sort_key):
     if not code or not code.strip(): continue
     lookup_sql_statements.append(f"INSERT INTO {SCHEMA_NAME}.certification_types_unimarc (certification_type_code, certification_type_name) VALUES ('{escape_sql_string(code.strip())}', '{escape_sql_string(name)}') ON CONFLICT (certification_type_code) DO UPDATE SET certification_type_name = EXCLUDED.certification_type_name;")

# Certifiers (Requires UNIQUE(certifier_name)) - Simplified based on name key
lookup_sql_statements.append("\n-- Certifiers (Requires UNIQUE(certifier_name). json_certifier_id might not be unique if different IDs share a name.)")
# Sort the collected candidates by name for deterministic output
sorted_certifier_names = sorted(certifier_candidates_by_name.keys())

for name in sorted_certifier_names:
    details = certifier_candidates_by_name[name]
    json_id = details['json_id']
    logo_url = details['logo_url']

    json_id_sql = json_id if json_id is not None else 'NULL'
    name_sql_val = name.strip() if name else None # Should always have a name due to how candidates were collected
    name_sql_for_insert = f"'{escape_sql_string(name_sql_val)}'" if name_sql_val is not None else 'NULL'
    logo_sql = f"'{escape_sql_string(logo_url)}'" if logo_url is not None else 'NULL'

    # Insert/Update based on the unique certifier_name
    lookup_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.certifiers_unimarc (json_certifier_id, certifier_name, certifier_logo_url)
    VALUES ({json_id_sql}, {name_sql_for_insert}, {logo_sql})
    ON CONFLICT (certifier_name) DO UPDATE SET
        json_certifier_id = COALESCE(EXCLUDED.json_certifier_id, {SCHEMA_NAME}.certifiers_unimarc.json_certifier_id),
        certifier_logo_url = COALESCE(EXCLUDED.certifier_logo_url, {SCHEMA_NAME}.certifiers_unimarc.certifier_logo_url);""")


# Certification Degrees
lookup_sql_statements.append("\n-- Certification Degrees (Requires UNIQUE(certification_degree_id))")
cert_degree_sort_key = lambda x: (x[0] if x[0] is not None else -1, str(x[1]) if x[1] is not None else "")
for degree_id, degree_name in sorted(list(unique_cert_degrees), key=cert_degree_sort_key):
     if degree_id is None or not degree_name or not degree_name.strip():
         print(f"Warning: Skipping certification degree with ID {degree_id} and name '{degree_name}' due to missing/empty required field.")
         continue
     lookup_sql_statements.append(f"INSERT INTO {SCHEMA_NAME}.certification_degrees_unimarc (certification_degree_id, certification_degree_name) VALUES ({degree_id}, '{escape_sql_string(degree_name.strip())}') ON CONFLICT (certification_degree_id) DO UPDATE SET certification_degree_name = EXCLUDED.certification_degree_name;")

# Countries
lookup_sql_statements.append("\n-- Countries (Requires UNIQUE(country_id))")
country_sort_key = lambda x: (x[0] if x[0] is not None else -1, str(x[1]) if x[1] is not None else "")
for country_id, country_name in sorted(list(unique_countries), key=country_sort_key):
     if country_id is None or not country_name or not country_name.strip():
         print(f"Warning: Skipping country with ID {country_id} and name '{country_name}' due to missing/empty required field.")
         continue
     lookup_sql_statements.append(f"INSERT INTO {SCHEMA_NAME}.countries_unimarc (country_id, country_name) VALUES ({country_id}, '{escape_sql_string(country_name.strip())}') ON CONFLICT (country_id) DO UPDATE SET country_name = EXCLUDED.country_name;")

# Write the lookup tables SQL file
lookup_output_filename = f"00_populate_unimarc_lookup_tables_from_{BASE_INPUT_NAME}.sql"
write_sql_file(lookup_output_filename, lookup_sql_statements)


# --- Generate SQL for Product Specific Data (Batched) ---
print(f"\nGenerating SQL for {len(product_raw_data_list)} products in batches of {PRODUCTS_PER_SQL_FILE}...")
product_batch_sql_statements = []
file_counter = 1

for i, product_data in enumerate(product_raw_data_list):
    ean = product_data['ean']
    item_data = product_data['item']
    price_data = product_data['price']
    promotion_data = product_data['promotion']
    ean_detail_data = product_data['ean_data']

    if not product_batch_sql_statements:
        product_batch_sql_statements.append(f"-- SQL script to populate Unimarc PRODUCT BATCH {file_counter}")
        product_batch_sql_statements.append(f"-- Generated by populate_sql.py from {SELECTED_JSON_FILENAME}")
        product_batch_sql_statements.append(f"-- Target Schema: {SCHEMA_NAME}")
        product_batch_sql_statements.append("-- UPSERT strategy for most tables. Stale collection items (images, ingredients, certifications) are cleared and re-inserted.")
        product_batch_sql_statements.append("-- IMPORTANT: ON CONFLICT requires corresponding UNIQUE constraints or PRIMARY KEYs.")
        product_batch_sql_statements.append("\n-- --- Product Specific Data ---")

    product_batch_sql_statements.append(f"\n-- Product: {ean}")

    # Skip products with missing or empty EAN - should not happen based on collection logic, but safe check
    if not ean or not str(ean).strip():
         print(f"Warning: Skipping product entry due to missing or empty EAN.")
         continue

    prod_id = safe_get(item_data, ['productId'])
    # If product ID is None from item data, try EAN detail data
    if prod_id is None and ean_detail_data: prod_id = safe_get(ean_detail_data, ['product_id'])
    prod_id_sql = f"'{escape_sql_string(prod_id)}'" if prod_id is not None else 'NULL' # Assuming prod_id might be a string in source? If always integer, remove quotes.

    item_id_val = safe_get(item_data, ['itemId'])
    sku = safe_get(item_data, ['sku'])
    name_val = safe_get(item_data, ['nameComplete']) or safe_get(item_data, ['name'])
    brand_id_val = safe_get(item_data, ['brandId'])
    category_id_val = safe_get(item_data, ['categoryId'])
    description = safe_get(item_data, ['descriptionShort']) or safe_get(item_data, ['description'])
    full_description = safe_get(ean_detail_data, ['full_description']) if ean_detail_data else None
    flavor = safe_get(ean_detail_data, ['flavor']) if ean_detail_data else None
    net_content = safe_get(item_data, ['netContent'])
    size_value = safe_get(ean_detail_data, ['size_value']) if ean_detail_data else None
    size_unit_name = safe_get(ean_detail_data, ['size_unit_name']) if ean_detail_data else None
    drained_size_value = safe_get(ean_detail_data, ['drained_size_value']) if ean_detail_data else None
    packaging_type_name = safe_get(ean_detail_data, ['packaging_type_name']) if ean_detail_data else None
    origin_country_name = safe_get(ean_detail_data, ['origin_country_name']) if ean_detail_data else None
    # Assuming timestamps are integers/numbers
    timestamp_in = safe_get(ean_detail_data, ['product_timestamp_in']) if ean_detail_data else None
    last_review = safe_get(ean_detail_data, ['product_last_review']) if ean_detail_data else None
    last_update = safe_get(ean_detail_data, ['product_last_update']) if ean_detail_data else None

    product_name_sql_val = name_val.strip() if name_val else None
    if product_name_sql_val is None or not product_name_sql_val:
         print(f"Warning: Skipping insertion/update for product EAN {ean} into {SCHEMA_NAME}.products_unimarc due to missing or empty name.")
         # We should still process its related data if available, but maybe skip the main product insert?
         # For now, let's skip the whole product if the name is essential (like for product table).
         # If you want to insert partial data, you'd need to change this continue.
         # Let's check if we have EAN detail data or item data, if not, it's truly incomplete.
         if not ean_detail_data and not item_data:
             print(f"Skipping product EAN {ean} completely due to lack of item/detail data.")
             continue # Skip to next product

         # If we have data but just no name, log warning and continue processing relations
         pass # Continue to process related tables like prices, promotions, etc.

    if product_name_sql_val is not None: # Only attempt product insert if name exists
        product_name_sql = f"'{escape_sql_string(product_name_sql_val)}'"
        product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.products_unimarc (
            ean, product_id, item_id, sku, name, brand_id, category_id, description, full_description, flavor,
            net_content, size_value, size_unit_name, drained_size_value, packaging_type_name,
            origin_country_name, product_timestamp_in, product_last_review, product_last_update
        ) VALUES (
            '{ean}', {prod_id_sql}, {f"'{escape_sql_string(item_id_val)}'" if item_id_val is not None else 'NULL'}, {f"'{escape_sql_string(sku)}'" if sku is not None else 'NULL'},
            {product_name_sql}, {brand_id_val if brand_id_val is not None else 'NULL'}, {category_id_val if category_id_val is not None else 'NULL'},
            {f"'{escape_sql_string(description)}'" if description is not None else 'NULL'}, {f"'{escape_sql_string(full_description)}'" if full_description is not None else 'NULL'}, {f"'{escape_sql_string(flavor)}'" if flavor is not None else 'NULL'},
            {f"'{escape_sql_string(net_content)}'" if net_content is not None else 'NULL'}, {size_value if size_value is not None else 'NULL'}, {f"'{escape_sql_string(size_unit_name)}'" if size_unit_name is not None else 'NULL'},
            {drained_size_value if drained_size_value is not None else 'NULL'}, {f"'{escape_sql_string(packaging_type_name)}'" if packaging_type_name is not None else 'NULL'},
            {f"'{escape_sql_string(origin_country_name)}'" if origin_country_name is not None else 'NULL'}, {timestamp_in if timestamp_in is not None else 'NULL'}, {last_review if last_review is not None else 'NULL'}, {last_update if last_update is not None else 'NULL'}
        )
        ON CONFLICT (ean) DO UPDATE SET
            product_id = EXCLUDED.product_id, item_id = EXCLUDED.item_id, sku = EXCLUDED.sku, name = EXCLUDED.name,
            brand_id = EXCLUDED.brand_id, category_id = EXCLUDED.category_id, description = EXCLUDED.description,
            full_description = EXCLUDED.full_description, flavor = EXCLUDED.flavor, net_content = EXCLUDED.net_content,
            size_value = EXCLUDED.size_value, size_unit_name = EXCLUDED.size_unit_name, drained_size_value = EXCLUDED.drained_size_value,
            packaging_type_name = EXCLUDED.packaging_type_name, origin_country_name = EXCLUDED.origin_country_name,
            product_timestamp_in = EXCLUDED.product_timestamp_in, product_last_review = EXCLUDED.product_last_review,
            product_last_update = EXCLUDED.product_last_update;""")
    else:
         # If product name was None/empty, set name to NULL in the update statement if the row exists
         product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.products_unimarc (ean, product_id, item_id, sku, brand_id, category_id)
         VALUES (
             '{ean}', {prod_id_sql}, {f"'{escape_sql_string(item_id_val)}'" if item_id_val is not None else 'NULL'}, {f"'{escape_sql_string(sku)}'" if sku is not None else 'NULL'},
             {brand_id_val if brand_id_val is not None else 'NULL'}, {category_id_val if category_id_val is not None else 'NULL'}
         )
         ON CONFLICT (ean) DO UPDATE SET
             product_id = EXCLUDED.product_id, item_id = EXCLUDED.item_id, sku = EXCLUDED.sku,
             brand_id = EXCLUDED.brand_id, category_id = EXCLUDED.category_id,
             name = NULL; -- Explicitly set name to NULL if source was empty/None
         """)
         print(f"Note: Product EAN {ean} will have a NULL name in {SCHEMA_NAME}.products_unimarc.")


    if price_data:
        price_val = clean_price(safe_get(price_data, ['price']))
        list_price_val = clean_price(safe_get(price_data, ['listPrice']))
        price_without_discount_val = clean_price(safe_get(price_data, ['priceWithoutDiscount']))
        reward_value = safe_get(price_data, ['rewardValue'])
        available_quantity = safe_get(price_data, ['availableQuantity'])
        in_offer = safe_get(price_data, ['inOffer'])
        ppum = safe_get(price_data, ['ppum'])
        ppum_list_price = safe_get(price_data, ['ppumListPrice'])
        saving = safe_get(price_data, ['saving'])
        product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_prices_unimarc (
            product_ean, price, list_price, price_without_discount, reward_value,
            available_quantity, in_offer, ppum, ppum_list_price, saving
        ) VALUES (
            '{ean}', {price_val if price_val is not None else 'NULL'}, {list_price_val if list_price_val is not None else 'NULL'},
            {price_without_discount_val if price_without_discount_val is not None else 'NULL'}, {reward_value if reward_value is not None else 'NULL'},
            {available_quantity if available_quantity is not None else 'NULL'}, {boolean_to_sql(in_offer)},
            {f"'{escape_sql_string(ppum)}'" if ppum is not None else 'NULL'}, {f"'{escape_sql_string(ppum_list_price)}'" if ppum_list_price is not None else 'NULL'},
            {f"'{escape_sql_string(saving)}'" if saving is not None else 'NULL'}
        )
        ON CONFLICT (product_ean) DO UPDATE SET
            price = EXCLUDED.price, list_price = EXCLUDED.list_price, price_without_discount = EXCLUDED.price_without_discount,
            reward_value = EXCLUDED.reward_value, available_quantity = EXCLUDED.available_quantity, in_offer = EXCLUDED.in_offer,
            ppum = EXCLUDED.ppum, ppum_list_price = EXCLUDED.ppum_list_price, saving = EXCLUDED.saving,
            last_updated = CURRENT_TIMESTAMP;""")

    if promotion_data:
         promo_id_val = safe_get(promotion_data, ['id'])
         promo_name = safe_get(promotion_data, ['name'])
         promo_type = safe_get(promotion_data, ['type'])
         has_savings = safe_get(promotion_data, ['hasSavings'])
         saving_str = safe_get(promotion_data, ['saving'])
         saving_val_cleaned = clean_price(saving_str)
         offer_message = safe_get(promotion_data, ['offerMessage'])
         description_message = safe_get(promotion_data, ['descriptionMessage'])
         product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_promotions_unimarc (
            product_ean, promotion_id, promotion_name, promotion_type, has_savings,
            saving, offer_message, description_message
         ) VALUES (
            '{ean}', {f"'{escape_sql_string(promo_id_val)}'" if promo_id_val is not None else 'NULL'}, {f"'{escape_sql_string(promo_name)}'" if promo_name is not None else 'NULL'},
            {f"'{escape_sql_string(promo_type)}'" if promo_type is not None else 'NULL'}, {boolean_to_sql(has_savings)},
            {saving_val_cleaned if saving_val_cleaned is not None else 'NULL'}, {boolean_to_sql(offer_message)},
            {f"'{escape_sql_string(description_message)}'" if description_message is not None else 'NULL'}
         )
         ON CONFLICT (product_ean) DO UPDATE SET
            promotion_id = EXCLUDED.promotion_id, promotion_name = EXCLUDED.promotion_name, promotion_type = EXCLUDED.promotion_type,
            has_savings = EXCLUDED.has_savings, saving = EXCLUDED.saving, offer_message = EXCLUDED.offer_message,
            description_message = EXCLUDED.description_message, last_updated = CURRENT_TIMESTAMP;""")

    images = safe_get(item_data, ['images'], [])
    # Clear existing images for this product before re-inserting
    product_batch_sql_statements.append(f"DELETE FROM {SCHEMA_NAME}.product_images_unimarc WHERE product_ean = '{ean}';")
    for img_idx, image_url in enumerate(images):
         if image_url and image_url.strip():
              product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_images_unimarc (product_ean, image_url, image_order)
                VALUES ('{ean}', '{escape_sql_string(image_url)}', {img_idx});""") # No ON CONFLICT needed after DELETE


    if ean_detail_data:
        # Ingredients, Allergens, Traces
        # Clear existing ingredient/allergen/trace relations for this product
        product_batch_sql_statements.append(f"DELETE FROM {SCHEMA_NAME}.product_ingredients_unimarc WHERE product_ean = '{ean}';")
        product_batch_sql_statements.append(f"DELETE FROM {SCHEMA_NAME}.product_allergens_unimarc WHERE product_ean = '{ean}';")
        product_batch_sql_statements.append(f"DELETE FROM {SCHEMA_NAME}.product_traces_unimarc WHERE product_ean = '{ean}';")

        ingredients_sets = safe_get(ean_detail_data, ['ingredients_sets'], [])
        all_ingredients_list = []
        for ing_set in ingredients_sets: all_ingredients_list.extend(safe_get(ing_set, ['ingredients'], []))
        for ing_idx, ing in enumerate(all_ingredients_list):
            ing_name_val = safe_get(ing, ['ingredient_name'])
            if ing_name_val and ing_name_val.strip():
                cleaned_ing_name = ing_name_val.strip()
                # Subquery relies on ingredient_name being unique in ingredients_unimarc (due to its ON CONFLICT rule)
                product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_ingredients_unimarc (product_ean, ingredient_lookup_id, ingredient_order)
                    VALUES (
                        '{ean}',
                        (SELECT ingredient_lookup_id FROM {SCHEMA_NAME}.ingredients_unimarc WHERE ingredient_name = '{escape_sql_string(cleaned_ing_name)}' LIMIT 1),
                        {ing_idx}
                    );""") # No ON CONFLICT needed after DELETE


        allergens_list_data = safe_get(ean_detail_data, ['allergens'], [])
        for ing in allergens_list_data:
             ing_name_val = safe_get(ing, ['ingredient_name'])
             if ing_name_val and ing_name_val.strip():
                cleaned_ing_name = ing_name_val.strip()
                product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_allergens_unimarc (product_ean, ingredient_lookup_id)
                    VALUES (
                        '{ean}',
                        (SELECT ingredient_lookup_id FROM {SCHEMA_NAME}.ingredients_unimarc WHERE ingredient_name = '{escape_sql_string(cleaned_ing_name)}' LIMIT 1)
                    );""") # No ON CONFLICT needed after DELETE


        traces_list_data = safe_get(ean_detail_data, ['traces'], [])
        for ing in traces_list_data:
             ing_name_val = safe_get(ing, ['ingredient_name'])
             if ing_name_val and ing_name_val.strip():
                 cleaned_ing_name = ing_name_val.strip()
                 product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_traces_unimarc (product_ean, ingredient_lookup_id)
                     VALUES (
                         '{ean}',
                         (SELECT ingredient_lookup_id FROM {SCHEMA_NAME}.ingredients_unimarc WHERE ingredient_name = '{escape_sql_string(cleaned_ing_name)}' LIMIT 1)
                     );""") # No ON CONFLICT needed after DELETE

        # Nutritional Info
        nutri_tables = safe_get(ean_detail_data, ['nutritional_tables_sets'])
        if nutri_tables:
            # Serving Info (UPSERT)
            portion_text = safe_get(nutri_tables, ['portionText'])
            portion_value = safe_get(nutri_tables, ['portionValue'])
            portion_unit = safe_get(nutri_tables, ['portionUnit'])
            num_portions = safe_get(nutri_tables, ['numPortions'])
            basic_unit = safe_get(nutri_tables, ['basicUnit'])
            product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_serving_info_unimarc (
                 product_ean, portion_text, portion_value, portion_unit, num_portions, basic_unit
            ) VALUES (
                 '{ean}', {f"'{escape_sql_string(portion_text)}'" if portion_text is not None else 'NULL'}, {portion_value if portion_value is not None else 'NULL'},
                 {f"'{escape_sql_string(portion_unit)}'" if portion_unit is not None else 'NULL'}, {num_portions if num_portions is not None else 'NULL'}, {f"'{escape_sql_string(basic_unit)}'" if basic_unit is not None else 'NULL'}
            )
            ON CONFLICT (product_ean) DO UPDATE SET
                portion_text = EXCLUDED.portion_text, portion_value = EXCLUDED.portion_value, portion_unit = EXCLUDED.portion_unit,
                num_portions = EXCLUDED.num_portions, basic_unit = EXCLUDED.basic_unit;""")

            # Nutritional Values (Clear & Re-insert)
            product_batch_sql_statements.append(f"DELETE FROM {SCHEMA_NAME}.product_nutritional_info_unimarc WHERE product_ean = '{ean}';")
            nutri_info = safe_get(nutri_tables, ['nutritionalInfo'], [])
            flat_nutri_info = flatten_nutri_nodes(nutri_info)
            for nutri_item in flat_nutri_info:
                name_val = nutri_item.get('name')
                value_100g = nutri_item.get('value_100g')
                value_portion = nutri_item.get('value_portion')
                if name_val and name_val.strip():
                    cleaned_name = name_val.strip()
                    # Subquery relies on name being unique in nutritional_info_types_unimarc
                    product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_nutritional_info_unimarc
                        (product_ean, nutritional_type_id, value_per_100g, value_per_portion)
                        VALUES (
                            '{ean}',
                            (SELECT nutritional_type_id FROM {SCHEMA_NAME}.nutritional_info_types_unimarc WHERE name = '{escape_sql_string(cleaned_name)}' LIMIT 1),
                            {value_100g if value_100g is not None else 'NULL'},
                            {value_portion if value_portion is not None else 'NULL'}
                        );""") # No ON CONFLICT needed after DELETE

        # Certifications (Clear & Re-insert)
        certificates = safe_get(ean_detail_data, ['certificates'], [])
        product_batch_sql_statements.append(f"DELETE FROM {SCHEMA_NAME}.product_certifications_unimarc WHERE product_ean = '{ean}';")
        for cert in certificates:
            type_code = safe_get(cert, ['certification_type_code'])
            if not type_code or not type_code.strip(): continue # Certification type code is essential
            certifiers_list_inner = safe_get(cert, ['certifiers'], [])
            for certifier_instance in certifiers_list_inner:
                certifier_json_id = safe_get(certifier_instance, ['certifier_id'])
                certifier_name_val = safe_get(certifier_instance, ['certifier_name'])
                degree_id_val = safe_get(certifier_instance, ['certification_degree_id'])
                country_id_val = safe_get(certifier_instance, ['certification_country_id'])
                cert_start = safe_get(certifier_instance, ['certification_start'])
                cert_end = safe_get(certifier_instance, ['certification_end'])
                cert_comments = safe_get(certifier_instance, ['certification_comments'])
                cert_last_update = safe_get(certifier_instance, ['certification_last_update'])

                # Ensure essential lookup IDs are available (degree, country)
                if degree_id_val is None or country_id_val is None:
                    print(f"Warning: Skipping a certification instance for EAN {ean} due to missing Degree ID ({degree_id_val}) or Country ID ({country_id_val}).")
                    continue

                # Build certifier lookup subquery
                certifier_lookup_sql = 'NULL' # Default to NULL if no certifier can be linked
                if certifier_name_val and certifier_name_val.strip():
                    cleaned_certifier_name = certifier_name_val.strip()
                    # Subquery relies on certifier_name being unique in certifiers_unimarc
                    certifier_lookup_sql = f"(SELECT certifier_id FROM {SCHEMA_NAME}.certifiers_unimarc WHERE certifier_name = '{escape_sql_string(cleaned_certifier_name)}' LIMIT 1)"
                elif certifier_json_id is not None and certifier_json_id != 0:
                     # Fallback lookup by json_certifier_id if name is missing/empty
                     # This requires json_certifier_id to be UNIQUE or have an index for the lookup to be performant/correct
                     certifier_lookup_sql = f"(SELECT certifier_id FROM {SCHEMA_NAME}.certifiers_unimarc WHERE json_certifier_id = {certifier_json_id} AND json_certifier_id IS NOT NULL LIMIT 1)"
                     print(f"Note: Linking certifier for EAN {ean} using json_certifier_id {certifier_json_id} as name was empty.")


                # Check if the lookup values exist in the lookup tables (optional but safer)
                # This would require fetching lookup IDs *before* generating product SQL, or using EXISTS subqueries.
                # For simplicity here, we rely on the lookup tables being populated first and the IDs existing.
                # The FK constraints in the DB will handle failures if IDs don't exist.

                product_batch_sql_statements.append(f"""INSERT INTO {SCHEMA_NAME}.product_certifications_unimarc (
                    product_ean, certification_type_code, certifier_id, certification_degree_id,
                    certification_country_id, certification_start, certification_end,
                    certification_comments, certification_last_update
                ) VALUES (
                    '{ean}', '{escape_sql_string(type_code.strip())}', {certifier_lookup_sql}, {degree_id_val}, {country_id_val},
                    {cert_start if cert_start is not None else 'NULL'}, {cert_end if cert_end is not None else 'NULL'},
                    {f"'{escape_sql_string(cert_comments)}'" if cert_comments is not None else 'NULL'}, {cert_last_update if cert_last_update is not None else 'NULL'}
                );""") # No ON CONFLICT needed after DELETE


    # Check if it's time to write a batch file
    if (i + 1) % PRODUCTS_PER_SQL_FILE == 0 or (i + 1) == len(product_raw_data_list):
        # Only write if there are actual statements in the current batch (e.g., after skipping products)
        # A batch with only headers and BEGIN/COMMIT is > 5 lines
        if product_batch_sql_statements and len(product_batch_sql_statements) > 5:
            batch_filename = f"{str(file_counter).zfill(2)}_populate_unimarc_products_batch_{file_counter}_from_{BASE_INPUT_NAME}.sql"
            write_sql_file(batch_filename, product_batch_sql_statements)
            product_batch_sql_statements = [] # Reset for the next batch
            file_counter += 1

print("\nFinished generating all SQL statements.")
# Final check if the last batch had any statements
if product_batch_sql_statements:
    if len(product_batch_sql_statements) > 5:
        batch_filename = f"{str(file_counter).zfill(2)}_populate_unimarc_products_batch_{file_counter}_from_{BASE_INPUT_NAME}.sql"
        write_sql_file(batch_filename, product_batch_sql_statements)
    else:
        print("Last batch was empty or contained only headers, no final file written.")
elif not product_raw_data_list and not lookup_sql_statements:
     print("No data processed, no SQL files generated.")
elif not product_raw_data_list and lookup_sql_statements:
     print(f"Only lookup table SQL generated (or attempted): {lookup_output_filename}. No product data found.")