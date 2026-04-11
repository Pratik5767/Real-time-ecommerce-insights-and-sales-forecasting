IF NOT EXISTS (
    SELECT 1 FROM sys.tables 
    WHERE name = 'gold_orders' AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    SELECT * 
    INTO dbo.gold_orders
    FROM lh_ecommerce_orders.gold.orders
    WHERE 1 = 0  
END
MERGE dbo.gold_orders AS target
USING (
    SELECT 
        window_start,
        window_end,
        state,
        total_sales,
        total_items
    FROM lh_ecommerce_orders.gold.orders
) AS source
ON (
    target.window_start = source.window_start
    AND target.window_end = source.window_end
    AND target.state = source.state
)
WHEN MATCHED THEN
    UPDATE SET
        target.total_sales  = source.total_sales,
        target.total_items  = source.total_items
WHEN NOT MATCHED BY TARGET THEN
    INSERT (window_start, window_end, state, total_sales, total_items)
    VALUES (source.window_start, source.window_end, source.state, source.total_sales, source.total_items);