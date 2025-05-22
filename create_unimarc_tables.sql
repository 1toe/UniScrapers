-- Drop tables in dependency order to avoid foreign key issues if rerunning
DROP TABLE IF EXISTS product_certifications_unimarc CASCADE;
DROP TABLE IF EXISTS certification_degrees_unimarc CASCADE;
DROP TABLE IF EXISTS certification_types_unimarc CASCADE;
DROP TABLE IF EXISTS certifiers_unimarc CASCADE;
DROP TABLE IF EXISTS countries_unimarc CASCADE;
DROP TABLE IF EXISTS product_nutritional_info_unimarc CASCADE;
DROP TABLE IF EXISTS nutritional_info_types_unimarc CASCADE;
DROP TABLE IF EXISTS product_serving_info_unimarc CASCADE;
DROP TABLE IF EXISTS product_ingredients_unimarc CASCADE;
DROP TABLE IF EXISTS product_allergens_unimarc CASCADE;
DROP TABLE IF EXISTS product_traces_unimarc CASCADE;
DROP TABLE IF EXISTS ingredients_unimarc CASCADE; -- Used for ingredients, allergens, traces names
DROP TABLE IF EXISTS product_images_unimarc CASCADE;
DROP TABLE IF EXISTS product_promotions_unimarc CASCADE;
DROP TABLE IF EXISTS product_prices_unimarc CASCADE;
DROP TABLE IF EXISTS products_unimarc CASCADE;
DROP TABLE IF EXISTS categories_unimarc CASCADE;
DROP TABLE IF EXISTS brands_unimarc CASCADE;

-- Add CASCADE to DROP TABLE statements for PostgreSQL
-- This helps handle foreign key dependencies automatically when dropping.
-- Use with caution, ensure you understand the consequences.
-- If using MySQL, remove CASCADE and ensure correct drop order.


-- 1. Brands Table
-- Stores information about product brands.
CREATE TABLE brands_unimarc (
    brand_id INT PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE NOT NULL
);

-- 2. Categories Table
-- Stores product categories, potentially with a hierarchy.
CREATE TABLE categories_unimarc (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE NOT NULL,
    category_slug VARCHAR(255) UNIQUE NULL, -- Slug for URL representation
    parent_category_id INT NULL, -- For hierarchy (self-referencing FK)
    FOREIGN KEY (parent_category_id) REFERENCES categories_unimarc(category_id)
);

-- 3. Products Table
-- The core table for product information.
CREATE TABLE products_unimarc (
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
    product_timestamp_in BIGINT NULL, -- Add timestamps back as they were in schema 1
    product_last_review BIGINT NULL,
    product_last_update BIGINT NULL,

    FOREIGN KEY (brand_id) REFERENCES brands_unimarc(brand_id),
    FOREIGN KEY (category_id) REFERENCES categories_unimarc(category_id)
);

-- 4. Product Prices Table
-- Records pricing and availability.
CREATE TABLE product_prices_unimarc (
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

    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE
);

-- 5. Product Promotions Table
-- Stores information about promotions.
CREATE TABLE product_promotions_unimarc (
    product_ean VARCHAR(13) PRIMARY KEY, -- Link promotion details directly to product (latest promotion)
    promotion_id VARCHAR(255) NULL, -- Promotion ID from the first source
    promotion_name VARCHAR(255) NULL,
    promotion_type VARCHAR(255) NULL,
    has_savings BOOLEAN NULL,
    saving DECIMAL(10, 2) NULL, -- If saving can be parsed to a number
    offer_message BOOLEAN NULL,
    description_message VARCHAR(255) NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE
);

-- 6. Product Images Table
-- Stores URLs for product images.
CREATE TABLE product_images_unimarc (
    image_id SERIAL PRIMARY KEY, -- Auto-incrementing primary key
    product_ean VARCHAR(13) NOT NULL,
    image_url VARCHAR(512) NOT NULL,
    image_order INT NULL, -- If the order of images matters

    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE
);

-- 7. Ingredients Table (and also used for Allergens/Traces names)
-- Stores unique names for ingredients, allergens, and trace items.
CREATE TABLE ingredients_unimarc (
    ingredient_lookup_id SERIAL PRIMARY KEY, -- Auto-generated PK
    json_ingredient_id VARCHAR(255) UNIQUE NULL, -- Original JSON ID for reference
    ingredient_name VARCHAR(255) UNIQUE NOT NULL -- e.g., "sémola de trigo duro", "gluten", "derivados lácteos"
);

-- 8. Product Ingredients Link Table (Many-to-Many)
-- Links products to their listed ingredients.
CREATE TABLE product_ingredients_unimarc (
    product_ean VARCHAR(13) NOT NULL,
    ingredient_lookup_id INT NOT NULL, -- FK to ingredients_unimarc table
    ingredient_order INT NULL, -- Order in the ingredients list

    PRIMARY KEY (product_ean, ingredient_lookup_id), -- Composite primary key
    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (ingredient_lookup_id) REFERENCES ingredients_unimarc(ingredient_lookup_id)
);

-- 9. Product Allergens Link Table (Many-to-Many)
-- Links products to their listed allergens.
CREATE TABLE product_allergens_unimarc (
    product_ean VARCHAR(13) NOT NULL,
    ingredient_lookup_id INT NOT NULL, -- FK to ingredients_unimarc table (allergen name)

    PRIMARY KEY (product_ean, ingredient_lookup_id), -- Composite primary key
    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (ingredient_lookup_id) REFERENCES ingredients_unimarc(ingredient_lookup_id)
);

-- 10. Product Traces Link Table (Many-to-Many)
-- Links products to their listed traces.
CREATE TABLE product_traces_unimarc (
    product_ean VARCHAR(13) NOT NULL,
    ingredient_lookup_id INT NOT NULL, -- FK to ingredients_unimarc table (trace name)

    PRIMARY KEY (product_ean, ingredient_lookup_id), -- Composite primary key
    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (ingredient_lookup_id) REFERENCES ingredients_unimarc(ingredient_lookup_id)
);

-- 11. Nutritional Info Types Table
-- Stores the definition of each nutrient.
CREATE TABLE nutritional_info_types_unimarc (
    nutritional_type_id SERIAL PRIMARY KEY, -- Auto-incrementing PK
    name VARCHAR(255) UNIQUE NOT NULL, -- Nutrient name
    unit VARCHAR(50) NULL, -- e.g., "kCal", "g", "mg"
    parent_nutritional_type_id INT NULL, -- For hierarchy (FK to self)

    FOREIGN KEY (parent_nutritional_type_id) REFERENCES nutritional_info_types_unimarc(nutritional_type_id)
);

-- 12. Product Nutritional Info Table
-- Stores the specific nutritional values for each product and nutrient type.
CREATE TABLE product_nutritional_info_unimarc (
    product_nutrition_id SERIAL PRIMARY KEY, -- Auto-incrementing PK
    product_ean VARCHAR(13) NOT NULL,
    nutritional_type_id INT NOT NULL, -- FK to nutritional_info_types_unimarc
    value_per_100g DECIMAL(10, 3) NULL, -- energyValue from JSON
    value_per_portion DECIMAL(10, 3) NULL, -- energyValuePortion from JSON

    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (nutritional_type_id) REFERENCES nutritional_info_types_unimarc(nutritional_type_id),
    UNIQUE (product_ean, nutritional_type_id) -- A product has only one value per nutrient type
);

-- 13. Product Serving Info Table
-- Stores serving size details associated with the nutritional information.
CREATE TABLE product_serving_info_unimarc (
    product_ean VARCHAR(13) PRIMARY KEY, -- One serving info record per product
    portion_text VARCHAR(255) NULL,
    portion_value DECIMAL(10, 3) NULL,
    portion_unit VARCHAR(50) NULL,
    num_portions DECIMAL(10, 3) NULL,
    basic_unit VARCHAR(50) NULL, -- basicUnit from JSON

    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE
);

-- 14. Certification Types Table
-- Stores the types of certifications.
CREATE TABLE certification_types_unimarc (
    certification_type_code VARCHAR(255) PRIMARY KEY, -- Use code as PK
    certification_type_name VARCHAR(255) UNIQUE NOT NULL
);

-- 15. Certifiers Table
-- Stores information about organizations that issue certifications.
CREATE TABLE certifiers_unimarc (
    certifier_id SERIAL PRIMARY KEY, -- Auto-generated PK
    json_certifier_id INT UNIQUE NULL, -- Original JSON ID
    certifier_name VARCHAR(255) UNIQUE NULL,
    certifier_logo_url VARCHAR(512) NULL
);

-- 16. Certification Degrees Table
-- Stores the degree/status of the certification.
CREATE TABLE certification_degrees_unimarc (
    certification_degree_id INT PRIMARY KEY, -- Use ID as PK
    certification_degree_name VARCHAR(255) UNIQUE NOT NULL
);

-- 17. Countries Table
-- Stores country information.
CREATE TABLE countries_unimarc (
    country_id INT PRIMARY KEY, -- Use ID as PK
    country_name VARCHAR(255) UNIQUE NOT NULL
);

-- 18. Product Certifications Link Table
-- Links products to their certifications, including details about the certification instance.
CREATE TABLE product_certifications_unimarc (
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

    FOREIGN KEY (product_ean) REFERENCES products_unimarc(ean) ON DELETE CASCADE,
    FOREIGN KEY (certification_type_code) REFERENCES certification_types_unimarc(certification_type_code),
    FOREIGN KEY (certifier_id) REFERENCES certifiers_unimarc(certifier_id),
    FOREIGN KEY (certification_degree_id) REFERENCES certification_degrees_unimarc(certification_degree_id),
    FOREIGN KEY (certification_country_id) REFERENCES countries_unimarc(country_id)

);