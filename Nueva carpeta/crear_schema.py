import os

# --- Configuration ---
# Define the output filename for the SQL schema
OUTPUT_FILENAME = "create_unimarc_tables.sql"
SCHEMA_NAME = "public" # Define the schema name

# --- SQL Schema ---
# Store the full SQL schema as a multi-line string, using the SCHEMA_NAME variable
# We'll use string formatting to insert the schema name.
sql_schema_template = """
-- Drop tables in dependency order to avoid foreign key issues if rerunning
-- Using CASCADE for PostgreSQL to drop dependent objects (like foreign keys)
DROP TABLE IF EXISTS {schema_name}.product_certifications_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.certification_degrees_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.certification_types_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.certifiers_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.countries_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_nutritional_info_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.nutritional_info_types_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_serving_info_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_ingredients_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_allergens_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_traces_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.ingredients_unimarc CASCADE; -- Used for ingredients, allergens, traces names
DROP TABLE IF EXISTS {schema_name}.product_images_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_promotions_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.product_prices_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.products_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.categories_unimarc CASCADE;
DROP TABLE IF EXISTS {schema_name}.brands_unimarc CASCADE;


-- 1. Brands Table
-- Stores information about product brands.
CREATE TABLE {schema_name}.brands_unimarc (
    brand_id INT PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE NOT NULL
);

-- 2. Categories Table
-- Stores product categories, potentially with a hierarchy.
CREATE TABLE {schema_name}.categories_unimarc (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE NOT NULL,
    category_slug VARCHAR(255) UNIQUE NULL, -- Slug for URL representation
    parent_category_id INT NULL, -- For hierarchy (self-referencing FK)
    FOREIGN KEY (parent_category_id) REFERENCES {schema_name}.categories_unimarc(category_id)
);

-- 3. Products Table
-- The core table for product information.
CREATE TABLE {schema_name}.products_unimarc (
    ean VARCHAR(13) PRIMARY KEY, -- EAN as primary key
    product_id VARCHAR(255) NULL, -- product_id from the second source (if needed)
    item_id VARCHAR(255) NULL, -- itemId from the first source
    sku VARCHAR(255) NULL, -- sku from the first source
    name VARCHAR(255) NOT NULL, -- Could be item.nameComplete or item.name
    brand_id INT NULL, -- Link to brands table (FK) - NULLable just in case
    category_id INT NULL, -- Link to categories table (FK) - NULLable just in case
    description TEXT NULL, -- Short description
    full_description TEXT NULL, -- More detailed description
    flavor VARCHAR(255) NULL,
    net_content VARCHAR(100) NULL, -- e.g. "270 g"
    size_value DECIMAL(10, 3) NULL, -- e.g., 250
    size_unit_name VARCHAR(50) NULL, -- e.g., "g"
    drained_size_value DECIMAL(10, 3) NULL, -- e.g., 250
    packaging_type_name VARCHAR(100) NULL,
    origin_country_name VARCHAR(255) NULL, -- e.g., "Uruguay"
    product_timestamp_in BIGINT NULL,
    product_last_review BIGINT NULL,
    product_last_update BIGINT NULL,

    FOREIGN KEY (brand_id) REFERENCES {schema_name}.brands_unimarc(brand_id),
    FOREIGN KEY (category_id) REFERENCES {schema_name}.categories_unimarc(category_id)
);

-- 4. Product Prices Table
-- Records pricing and availability.
CREATE TABLE {schema_name}.product_prices_unimarc (
    product_ean VARCHAR(13) PRIMARY KEY, -- One price entry per product (latest)
    price DECIMAL(10, 2) NULL, -- Storing price as a number
    list_price DECIMAL(10, 2) NULL,
    price_without_discount DECIMAL(10, 2) NULL,
    reward_value INT NULL,
    available_quantity INT NULL,
    in_offer BOOLEAN NULL,
    ppum VARCHAR(100) NULL, -- Price per unit of measure
    ppum_list_price VARCHAR(100) NULL,
    saving VARCHAR(100) NULL, -- Keep saving as string
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE
);

-- 5. Product Promotions Table
-- Stores information about promotions.
CREATE TABLE {schema_name}.product_promotions_unimarc (
    product_ean VARCHAR(13) PRIMARY KEY, -- Link promotion details directly to product (latest promotion)
    promotion_id VARCHAR(255) NULL, -- Promotion ID from the first source
    promotion_name VARCHAR(255) NULL,
    promotion_type VARCHAR(255) NULL,
    has_savings BOOLEAN NULL,
    saving DECIMAL(10, 2) NULL, -- If saving can be parsed to a number
    offer_message BOOLEAN NULL,
    description_message VARCHAR(255) NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE
);

-- 6. Product Images Table
-- Stores URLs for product images.
CREATE TABLE {schema_name}.product_images_unimarc (
    image_id SERIAL PRIMARY KEY, -- Auto-incrementing primary key
    product_ean VARCHAR(13) NOT NULL,
    image_url VARCHAR(512) NOT NULL,
    image_order INT NULL, -- If the order of images matters

    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE
);

-- 7. Ingredients Table (and also used for Allergens/Traces names)
-- Stores unique names for ingredients, allergens, and trace items.
CREATE TABLE {schema_name}.ingredients_unimarc (
    ingredient_lookup_id SERIAL PRIMARY KEY, -- Auto-generated PK
    json_ingredient_id VARCHAR(255) UNIQUE NULL, -- Original JSON ID for reference
    ingredient_name VARCHAR(255) UNIQUE NOT NULL -- e.g., "sémola de trigo duro", "gluten", "derivados lácteos"
);

-- 8. Product Ingredients Link Table (Many-to-Many)
-- Links products to their listed ingredients.
CREATE TABLE {schema_name}.product_ingredients_unimarc (
    product_ean VARCHAR(13) NOT NULL,
    ingredient_lookup_id INT NOT NULL, -- FK to ingredients_unimarc table
    ingredient_order INT NULL, -- Order in the ingredients list

    PRIMARY KEY (product_ean, ingredient_lookup_id), -- Composite primary key
    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (ingredient_lookup_id) REFERENCES {schema_name}.ingredients_unimarc(ingredient_lookup_id)
);

-- 9. Product Allergens Link Table (Many-to-Many)
-- Links products to their listed allergens.
CREATE TABLE {schema_name}.product_allergens_unimarc (
    product_ean VARCHAR(13) NOT NULL,
    ingredient_lookup_id INT NOT NULL, -- FK to ingredients_unimarc table (allergen name)

    PRIMARY KEY (product_ean, ingredient_lookup_id), -- Composite primary key
    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (ingredient_lookup_id) REFERENCES {schema_name}.ingredients_unimarc(ingredient_lookup_id)
);

-- 10. Product Traces Link Table (Many-to-Many)
-- Links products to their listed traces.
CREATE TABLE {schema_name}.product_traces_unimarc (
    product_ean VARCHAR(13) NOT NULL,
    ingredient_lookup_id INT NOT NULL, -- FK to ingredients_unimarc table (trace name)

    PRIMARY KEY (product_ean, ingredient_lookup_id), -- Composite primary key
    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (ingredient_lookup_id) REFERENCES {schema_name}.ingredients_unimarc(ingredient_lookup_id)
);

-- 11. Nutritional Info Types Table
-- Stores the definition of each nutrient.
CREATE TABLE {schema_name}.nutritional_info_types_unimarc (
    nutritional_type_id SERIAL PRIMARY KEY, -- Auto-incrementing PK
    name VARCHAR(255) UNIQUE NOT NULL, -- Nutrient name
    unit VARCHAR(50) NULL, -- e.g., "kCal", "g", "mg"
    parent_nutritional_type_id INT NULL, -- For hierarchy (FK to self)

    FOREIGN KEY (parent_nutritional_type_id) REFERENCES {schema_name}.nutritional_info_types_unimarc(nutritional_type_id)
);

-- 12. Product Nutritional Info Table
-- Stores the specific nutritional values for each product and nutrient type.
CREATE TABLE {schema_name}.product_nutritional_info_unimarc (
    product_nutrition_id SERIAL PRIMARY KEY, -- Auto-incrementing PK
    product_ean VARCHAR(13) NOT NULL,
    nutritional_type_id INT NOT NULL, -- FK to nutritional_info_types_unimarc
    value_per_100g DECIMAL(10, 3) NULL, -- energyValue from JSON
    value_per_portion DECIMAL(10, 3) NULL, -- energyValuePortion from JSON

    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (nutritional_type_id) REFERENCES {schema_name}.nutritional_info_types_unimarc(nutritional_type_id),
    UNIQUE (product_ean, nutritional_type_id) -- A product has only one value per nutrient type
);

-- 13. Product Serving Info Table
-- Stores serving size details associated with the nutritional information.
CREATE TABLE {schema_name}.product_serving_info_unimarc (
    product_ean VARCHAR(13) PRIMARY KEY, -- One serving info record per product
    portion_text VARCHAR(255) NULL,
    portion_value DECIMAL(10, 3) NULL,
    portion_unit VARCHAR(50) NULL,
    num_portions DECIMAL(10, 3) NULL,
    basic_unit VARCHAR(50) NULL, -- basicUnit from JSON

    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE
);

-- 14. Certification Types Table
-- Stores the types of certifications.
CREATE TABLE {schema_name}.certification_types_unimarc (
    certification_type_code VARCHAR(255) PRIMARY KEY, -- Use code as PK
    certification_type_name VARCHAR(255) UNIQUE NOT NULL
);

-- 15. Certifiers Table
-- Stores information about organizations that issue certifications.
CREATE TABLE {schema_name}.certifiers_unimarc (
    certifier_id SERIAL PRIMARY KEY, -- Auto-generated PK
    json_certifier_id INT UNIQUE NULL, -- Original JSON ID
    certifier_name VARCHAR(255) UNIQUE NULL,
    certifier_logo_url VARCHAR(512) NULL
);

-- 16. Certification Degrees Table
-- Stores the degree/status of the certification.
CREATE TABLE {schema_name}.certification_degrees_unimarc (
    certification_degree_id INT PRIMARY KEY, -- Use ID as PK
    certification_degree_name VARCHAR(255) UNIQUE NOT NULL
);

-- 17. Countries Table
-- Stores country information.
CREATE TABLE {schema_name}.countries_unimarc (
    country_id INT PRIMARY KEY, -- Use ID as PK
    country_name VARCHAR(255) UNIQUE NOT NULL
);

-- 18. Product Certifications Link Table
-- Links products to their certifications, including details about the certification instance.
CREATE TABLE {schema_name}.product_certifications_unimarc (
    product_certification_id SERIAL PRIMARY KEY, -- Auto-incrementing PK
    product_ean VARCHAR(13) NOT NULL,
    certification_type_code VARCHAR(255) NOT NULL, -- FK to certification_types_unimarc
    certifier_id INT NULL, -- FK to certifiers_unimarc (nullable)
    certification_degree_id INT NOT NULL, -- FK to certification_degrees_unimarc
    certification_country_id INT NOT NULL, -- FK to countries_unimarc
    certification_start BIGINT NULL, -- Timestamp
    certification_end BIGINT NULL, -- Timestamp
    certification_comments TEXT NULL,
    certification_last_update BIGINT NULL, -- Timestamp

    FOREIGN KEY (product_ean) REFERENCES {schema_name}.products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (certification_type_code) REFERENCES {schema_name}.certification_types_unimarc(certification_type_code),
    FOREIGN KEY (certifier_id) REFERENCES {schema_name}.certifiers_unimarc(certifier_id),
    FOREIGN KEY (certification_degree_id) REFERENCES {schema_name}.certification_degrees_unimarc(certification_degree_id),
    FOREIGN KEY (certification_country_id) REFERENCES {schema_name}.countries_unimarc(country_id)

);
"""

# --- Main Execution ---
print(f"Generating SQL schema file: {OUTPUT_FILENAME} for schema '{SCHEMA_NAME}'...")

try:
    # Format the schema string with the specified schema name
    sql_schema_formatted = sql_schema_template.format(schema_name=SCHEMA_NAME)

    # Open the output file in write mode, using utf-8 encoding
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        # Write the formatted SQL schema string to the file
        f.write(sql_schema_formatted.strip()) # .strip() to remove potential leading/trailing whitespace

    print(f"Successfully generated SQL schema file: {OUTPUT_FILENAME}")

except IOError as e:
    print(f"Error writing SQL file {OUTPUT_FILENAME}: {e}")
    # Exit with a non-zero status code to indicate an error
    exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    # Optional: print traceback for debugging
    import traceback
    traceback.print_exc()
    exit(1)

# Exit with a zero status code to indicate success
exit(0)