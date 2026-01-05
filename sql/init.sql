CREATE TABLE products (
    sku VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    safety_stock INT,
    min_order_quantity INT,
    pack_size INT,
    supplier_id VARCHAR(50)
);

CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    lead_time_days INT
);

-- =============================================
-- SUPPLIERS (5 major Moroccan suppliers)
-- =============================================
INSERT INTO suppliers VALUES ('SUP_DANONE', 'Centrale Danone', 1);
INSERT INTO suppliers VALUES ('SUP_LESIEUR', 'Lesieur Cristal', 2);
INSERT INTO suppliers VALUES ('SUP_COSUMAR', 'Cosumar', 3);
INSERT INTO suppliers VALUES ('SUP_BIMO', 'Bimo (Mondelez)', 2);
INSERT INTO suppliers VALUES ('SUP_COPAG', 'Copag (Jaouda)', 1);

-- =============================================
-- PRODUCTS (30 products across categories)
-- =============================================
-- Dairy (Danone)
INSERT INTO products VALUES ('P_101', 'Milk 1L', 50, 100, 12, 'SUP_DANONE');
INSERT INTO products VALUES ('P_102', 'Yogurt Strawberry', 30, 50, 24, 'SUP_DANONE');
INSERT INTO products VALUES ('P_103', 'Yogurt Vanilla', 30, 50, 24, 'SUP_DANONE');
INSERT INTO products VALUES ('P_104', 'Danette Chocolate', 20, 48, 12, 'SUP_DANONE');
INSERT INTO products VALUES ('P_105', 'Activia Natural', 25, 36, 6, 'SUP_DANONE');
INSERT INTO products VALUES ('P_106', 'Raibi Jamila', 40, 60, 12, 'SUP_DANONE');

-- Oils & Condiments (Lesieur)
INSERT INTO products VALUES ('P_201', 'Huile Tournesol 1L', 30, 24, 12, 'SUP_LESIEUR');
INSERT INTO products VALUES ('P_202', 'Huile Olive 500ml', 15, 12, 6, 'SUP_LESIEUR');
INSERT INTO products VALUES ('P_203', 'Mayonnaise 250g', 20, 24, 12, 'SUP_LESIEUR');
INSERT INTO products VALUES ('P_204', 'Ketchup 340g', 25, 24, 12, 'SUP_LESIEUR');
INSERT INTO products VALUES ('P_205', 'Moutarde 200g', 15, 24, 12, 'SUP_LESIEUR');

-- Sugar & Sweeteners (Cosumar)
INSERT INTO products VALUES ('P_301', 'Sucre Granule 1kg', 100, 50, 25, 'SUP_COSUMAR');
INSERT INTO products VALUES ('P_302', 'Sucre Morceaux 1kg', 60, 40, 20, 'SUP_COSUMAR');
INSERT INTO products VALUES ('P_303', 'Sucre Glace 500g', 25, 24, 12, 'SUP_COSUMAR');
INSERT INTO products VALUES ('P_304', 'Miel 500g', 15, 12, 6, 'SUP_COSUMAR');

-- Biscuits & Snacks (Bimo)
INSERT INTO products VALUES ('P_401', 'Tagger Chocolate', 40, 48, 24, 'SUP_BIMO');
INSERT INTO products VALUES ('P_402', 'Tagger Vanille', 35, 48, 24, 'SUP_BIMO');
INSERT INTO products VALUES ('P_403', 'Biscuit Petit Dejeuner', 30, 36, 12, 'SUP_BIMO');
INSERT INTO products VALUES ('P_404', 'Cookies Chocolat', 25, 24, 12, 'SUP_BIMO');
INSERT INTO products VALUES ('P_405', 'Gaufrettes Fraise', 20, 24, 12, 'SUP_BIMO');
INSERT INTO products VALUES ('P_406', 'Crackers Sales', 30, 36, 18, 'SUP_BIMO');

-- Juices & Dairy (Copag/Jaouda)
INSERT INTO products VALUES ('P_501', 'Lben 500ml', 60, 48, 12, 'SUP_COPAG');
INSERT INTO products VALUES ('P_502', 'Raib 500ml', 50, 48, 12, 'SUP_COPAG');
INSERT INTO products VALUES ('P_503', 'Jus Orange 1L', 40, 24, 12, 'SUP_COPAG');
INSERT INTO products VALUES ('P_504', 'Jus Pomme 1L', 30, 24, 12, 'SUP_COPAG');
INSERT INTO products VALUES ('P_505', 'Jus Multifruits 1L', 35, 24, 12, 'SUP_COPAG');
INSERT INTO products VALUES ('P_506', 'Lait Fermente 1L', 45, 36, 12, 'SUP_COPAG');