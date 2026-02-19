from dataclasses import dataclass
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dataclass
class SupplyChainSynthConfig:
    n_skus: int = 2000
    n_stores: int = 150
    n_dcs: int = 6
    n_suppliers: int = 120
    days: int = 180
    start_date: str = "2025-08-01"
    seed: int = 42

    # realism knobs
    promo_rate: float = 0.08              # probability store-sku-day is on promo
    base_stockout_rate: float = 0.02      # baseline prob of "at risk" signal
    supplier_late_rate: float = 0.12      # baseline late inbound probability
    supplier_short_rate: float = 0.06     # baseline short-ship probability

    # performance knobs
    sales_sparsity: float = 0.45          # keep fraction of store-sku pairs (reduces sales rows)
    avg_po_lines: int = 18                # avg lines per PO
    pos_per_day_per_dc: float = 1.2       # controls PO volume

def generate_supply_chain_data(spark, cfg: SupplyChainSynthConfig) -> Dict[str, DataFrame]:
    """
    Returns a dict of DataFrames:
      dims: dim_sku, dim_store, dim_dc, dim_supplier, dim_date
      facts: fact_sales_daily, fact_inventory_daily, fact_po, fact_po_line,
             fact_receipt, fact_receipt_line, fact_shipment
    """

    # ----------------------------
    # Helper: deterministic random
    # ----------------------------
    def r(seed_offset: int = 0):
        return F.rand(cfg.seed + seed_offset)

    # ----------------------------
    # Date dimension
    # ----------------------------
    dim_date = (
        spark.sql(f"SELECT sequence(to_date('{cfg.start_date}'), date_add(to_date('{cfg.start_date}'), {cfg.days - 1})) AS dts")
        .select(F.explode("dts").alias("date"))
        .withColumn("dow", F.date_format("date", "u").cast("int"))          # 1=Mon ... 7=Sun
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("month", F.month("date"))
    )

    # Create 2 "shock weeks" inside the range (defaults)
    shock1_start = F.date_add(F.to_date(F.lit(cfg.start_date)), 40)
    shock1_end   = F.date_add(F.to_date(F.lit(cfg.start_date)), 46)
    shock2_start = F.date_add(F.to_date(F.lit(cfg.start_date)), 110)
    shock2_end   = F.date_add(F.to_date(F.lit(cfg.start_date)), 116)

    dim_date = (
        dim_date
        .withColumn(
            "shock_flag",
            F.when((F.col("date") >= shock1_start) & (F.col("date") <= shock1_end), F.lit(1))
             .when((F.col("date") >= shock2_start) & (F.col("date") <= shock2_end), F.lit(1))
             .otherwise(F.lit(0))
        )
        .withColumn("demand_multiplier",
                    F.when(F.col("shock_flag") == 1, F.lit(1.35)).otherwise(F.lit(1.0)))
        .withColumn("delay_multiplier",
                    F.when(F.col("shock_flag") == 1, F.lit(1.6)).otherwise(F.lit(1.0)))
    )

    # ----------------------------
    # Dimensions
    # ----------------------------
    regions = ["BC", "AB", "SK/MB"]
    categories = ["produce", "dairy", "meat", "frozen", "pantry", "household"]

    # DCs: 2 per region if n_dcs=6
    dim_dc = (
        spark.range(cfg.n_dcs)
        .withColumn("dc_id", F.format_string("DC%02d", F.col("id") + 1))
        .withColumn("region", F.element_at(F.array([F.lit(x) for x in regions]), (F.col("id") % F.lit(len(regions))) + 1))
        .drop("id")
    )

    dim_store = (
        spark.range(cfg.n_stores)
        .withColumn("store_id", F.format_string("S%04d", F.col("id") + 1))
        .withColumn("region", F.element_at(F.array([F.lit(x) for x in regions]), (F.col("id") % F.lit(len(regions))) + 1))
        .withColumn("banner", F.when((F.col("id") % 3) == 0, F.lit("Save-On-Foods"))
                            .when((F.col("id") % 3) == 1, F.lit("PriceSmart Foods"))
                            .otherwise(F.lit("Urban Fare")))
        .withColumn("city", F.concat(F.lit("City-"), F.col("region"), F.lit("-"), (F.col("id") % 25).cast("string")))
        .drop("id")
    )

    # assign a DC to each store within the same region
    # (simple: choose DC based on hashed store_id among DCs in region)
    dc_by_region = dim_dc.groupBy("region").agg(F.collect_list("dc_id").alias("dc_list"))
    dim_store = (
        dim_store.join(dc_by_region, "region", "left")
        .withColumn("dc_id",
                    F.element_at(
                        F.col("dc_list"),
                        (F.pmod(F.xxhash64("store_id"), F.size("dc_list")) + 1).cast("int")
                    ))
        .drop("dc_list")
    )

    # Suppliers: reliability drives late + short rates
    dim_supplier = (
        spark.range(cfg.n_suppliers)
        .withColumn("supplier_id", F.format_string("SUP%04d", F.col("id") + 1))
        .withColumn("region", F.element_at(F.array([F.lit(x) for x in regions]), (F.col("id") % F.lit(len(regions))) + 1))
        .withColumn("reliability_score", (F.lit(0.65) + r(10) * F.lit(0.33)))  # 0.65–0.98
        .withColumn("late_rate",
                    F.greatest(F.lit(0.02),
                               F.lit(cfg.supplier_late_rate) * (F.lit(1.15) - F.col("reliability_score"))))
        .withColumn("short_rate",
                    F.greatest(F.lit(0.01),
                               F.lit(cfg.supplier_short_rate) * (F.lit(1.10) - F.col("reliability_score"))))
        .drop("id")
    )

    # SKUs: categories + perishable flags
    dim_sku = (
        spark.range(cfg.n_skus)
        .withColumn("sku_id", F.format_string("SKU%06d", F.col("id") + 1))
        .withColumn("category", F.element_at(F.array([F.lit(x) for x in categories]), (F.col("id") % F.lit(len(categories))) + 1))
        .withColumn("brand", F.concat(F.lit("Brand-"), (F.col("id") % 40).cast("string")))
        .withColumn("unit", F.when(F.col("category").isin("produce", "meat"), F.lit("kg")).otherwise(F.lit("unit")))
        .withColumn("case_pack", (F.lit(6) + (F.col("id") % 12)).cast("int"))
        .withColumn("is_perishable", F.col("category").isin("produce", "dairy", "meat"))
        .withColumn("shelf_life_days",
                    F.when(F.col("category") == "produce", F.lit(7))
                     .when(F.col("category") == "dairy", F.lit(14))
                     .when(F.col("category") == "meat", F.lit(10))
                     .when(F.col("category") == "frozen", F.lit(180))
                     .when(F.col("category") == "pantry", F.lit(365))
                     .otherwise(F.lit(540)))
        .withColumn("unit_cost",
                    F.round(
                        F.when(F.col("category") == "meat", 4.0 + r(20) * 12.0)
                         .when(F.col("category") == "produce", 1.0 + r(21) * 5.0)
                         .when(F.col("category") == "dairy", 1.5 + r(22) * 4.0)
                         .when(F.col("category") == "frozen", 2.5 + r(23) * 7.0)
                         .when(F.col("category") == "pantry", 0.8 + r(24) * 6.0)
                         .otherwise(1.2 + r(25) * 8.0),
                        2
                    ))
        .drop("id")
    )

    # Map each SKU to a primary supplier (simple, stable)
    sku_supplier = (
        dim_sku.select("sku_id")
        .withColumn("supplier_id",
                    F.format_string("SUP%04d", (F.pmod(F.xxhash64("sku_id"), F.lit(cfg.n_suppliers)) + 1).cast("int")))
    )

    dim_sku = dim_sku.join(sku_supplier, "sku_id", "left")

    # ----------------------------
    # FACT: Sales Daily
    # ----------------------------
    # Reduce row count by sampling store-sku pairs (sparsity)
    store_sku = (
        dim_store.select("store_id", "region", "dc_id")
        .crossJoin(dim_sku.select("sku_id", "category", "unit_cost"))
        .where(r(30) < F.lit(cfg.sales_sparsity))
    )

    fact_sales_daily = (
        store_sku.crossJoin(dim_date.select("date", "dow", "shock_flag", "demand_multiplier"))
        .withColumn("promo_flag", (r(31) < F.lit(cfg.promo_rate)).cast("int"))
        .withColumn("promo_multiplier", F.when(F.col("promo_flag") == 1, F.lit(1.25)).otherwise(F.lit(1.0)))
        .withColumn("weekend_multiplier", F.when(F.col("dow").isin(6, 7), F.lit(1.12)).otherwise(F.lit(1.0)))
        # base demand by category + noise
        .withColumn(
            "base_units",
            F.when(F.col("category") == "produce", 2.0 + r(32) * 10.0)
             .when(F.col("category") == "dairy", 1.5 + r(33) * 8.0)
             .when(F.col("category") == "meat", 0.8 + r(34) * 5.0)
             .when(F.col("category") == "frozen", 0.6 + r(35) * 4.0)
             .when(F.col("category") == "pantry", 0.9 + r(36) * 6.0)
             .otherwise(0.5 + r(37) * 3.0)
        )
        .withColumn("units_sold",
                    F.greatest(
                        F.lit(0),
                        F.round(F.col("base_units") * F.col("demand_multiplier") * F.col("promo_multiplier") * F.col("weekend_multiplier") * (0.6 + r(38) * 0.9), 0)
                    ).cast("int"))
        .withColumn("price",
                    F.round((F.col("unit_cost") * (1.35 + r(39) * 0.45)) *
                            F.when(F.col("promo_flag") == 1, F.lit(0.92)).otherwise(F.lit(1.0)), 2))
        .select("date", "store_id", "sku_id", "units_sold", "price", "promo_flag", "region", "dc_id", "shock_flag")
    )

    # ----------------------------
    # FACT: Inventory Daily at DC
    # ----------------------------
    # Keep this manageable: sku x dc x day
    dc_sku = dim_dc.select("dc_id", "region").crossJoin(dim_sku.select("sku_id", "category", "is_perishable"))
    fact_inventory_daily = (
        dc_sku.crossJoin(dim_date.select("date", "shock_flag"))
        # baseline inventory by category + noise
        .withColumn("target_on_hand",
                    F.when(F.col("category") == "produce", 200 + (r(40) * 200))
                     .when(F.col("category") == "dairy", 250 + (r(41) * 250))
                     .when(F.col("category") == "meat", 150 + (r(42) * 200))
                     .when(F.col("category") == "frozen", 300 + (r(43) * 400))
                     .when(F.col("category") == "pantry", 350 + (r(44) * 600))
                     .otherwise(220 + (r(45) * 350)))
        # shock weeks reduce on-hand (demand spikes + delays)
        .withColumn("shock_drawdown", F.when(F.col("shock_flag") == 1, F.lit(0.78)).otherwise(F.lit(1.0)))
        .withColumn("on_hand_units", F.round(F.col("target_on_hand") * F.col("shock_drawdown") * (0.7 + r(46) * 0.7), 0).cast("int"))
        .withColumn("on_order_units", F.round((F.col("target_on_hand") * 0.35) * (0.6 + r(47) * 0.9), 0).cast("int"))
        .select("date", "dc_id", "sku_id", "on_hand_units", "on_order_units", "region", "shock_flag")
    )

    # ----------------------------
    # FACT: Purchase Orders + Lines (supplier -> DC)
    # ----------------------------
    # Create a stream of POs per day per DC (small-ish), then generate lines per PO.
    total_pos = int(cfg.days * cfg.n_dcs * cfg.pos_per_day_per_dc)

    fact_po = (
        spark.range(total_pos)
        .withColumn("po_id", F.format_string("PO%09d", F.col("id") + 1))
        .withColumn("dc_id", F.format_string("DC%02d", (F.pmod(F.col("id"), F.lit(cfg.n_dcs)) + 1).cast("int")))
        .withColumn("order_date", F.expr(f"date_add(to_date('{cfg.start_date}'), cast(pmod(id, {cfg.days}) as int))"))
        .drop("id")
        .join(dim_dc.select("dc_id", "region").withColumnRenamed("region", "dc_region"), "dc_id", "left")
        # choose supplier biased to same region
        .withColumn("supplier_pick", r(50))
        .withColumn("supplier_region",
                    F.when(F.col("supplier_pick") < 0.7, F.col("dc_region"))  # 70% local sourcing
                     .otherwise(F.element_at(F.array([F.lit(x) for x in regions]), (F.pmod(F.xxhash64("po_id"), F.lit(len(regions))) + 1).cast("int"))))
        .drop("supplier_pick")
    )

    suppliers_by_region = dim_supplier.groupBy("region").agg(F.collect_list("supplier_id").alias("sup_list"))
    fact_po = (
        fact_po.join(suppliers_by_region.withColumnRenamed("region", "supplier_region"), "supplier_region", "left")
        .withColumn("supplier_id",
                    F.element_at(
                        F.col("sup_list"),
                        (F.pmod(F.xxhash64("po_id"), F.size("sup_list")) + 1).cast("int")
                    ))
        .drop("sup_list", "supplier_region")
        .join(dim_supplier.select("supplier_id", "late_rate", "short_rate", "reliability_score"), "supplier_id", "left")
        .join(dim_date.select(F.col("date").alias("order_date"), "delay_multiplier", "shock_flag"), "order_date", "left")
        # lead time: 2–10 days, stretched during shocks
        .withColumn("base_lead_days", (F.lit(2) + F.floor(r(51) * 9)).cast("int"))
        .withColumn("promised_lead_days", F.round(F.col("base_lead_days") * F.col("delay_multiplier"), 0).cast("int"))
        .withColumn("promised_date", F.expr("date_add(order_date, promised_lead_days)"))
        .withColumn("is_late", (r(52) < (F.col("late_rate") * F.col("delay_multiplier"))).cast("int"))
        .withColumn("actual_lead_days",
                    F.when(F.col("is_late") == 1,
                           (F.col("promised_lead_days") + (1 + F.floor(r(53) * 5)).cast("int")))
                     .otherwise(F.col("promised_lead_days")))
        .withColumn("received_date", F.expr("date_add(order_date, actual_lead_days)"))
        .withColumn("status", F.when(F.col("received_date") <= F.date_add(F.to_date(F.lit(cfg.start_date)), cfg.days - 1), F.lit("RECEIVED"))
                           .otherwise(F.lit("OPEN")))
        .select("po_id", "supplier_id", "dc_id", "dc_region", "order_date", "promised_date", "received_date", "status",
                "reliability_score", "late_rate", "short_rate", "shock_flag")
    )

    # PO Lines: attach SKUs (biased to categories but random) with contracted unit cost
    # Use explode(sequence) to create ~avg_po_lines per PO
    fact_po_line = (
        fact_po.select("po_id", "supplier_id", "dc_id", "order_date", "short_rate", "shock_flag")
        .withColumn("n_lines", F.greatest(F.lit(1), (F.lit(cfg.avg_po_lines) + F.floor((r(60) - 0.5) * 10)).cast("int")))
        .withColumn("line_seq", F.expr("sequence(1, n_lines)"))
        .select("po_id", "supplier_id", "dc_id", "order_date", "short_rate", "shock_flag", F.explode("line_seq").alias("line_n"))
        .drop("line_seq", "n_lines")
        # pick SKU per line
        .withColumn("sku_pick", (F.pmod(F.xxhash64("po_id", "line_n"), F.lit(cfg.n_skus)) + 1).cast("int"))
        .withColumn("sku_id", F.format_string("SKU%06d", F.col("sku_pick")))
        .drop("sku_pick", "line_n")
        .join(dim_sku.select("sku_id", "category", "unit_cost"), "sku_id", "left")
        .withColumn("ordered_units",
                    F.when(F.col("category") == "produce", (50 + F.floor(r(61) * 250)).cast("int"))
                     .when(F.col("category") == "dairy", (60 + F.floor(r(62) * 280)).cast("int"))
                     .when(F.col("category") == "meat", (30 + F.floor(r(63) * 160)).cast("int"))
                     .when(F.col("category") == "frozen", (40 + F.floor(r(64) * 220)).cast("int"))
                     .when(F.col("category") == "pantry", (80 + F.floor(r(65) * 420)).cast("int"))
                     .otherwise((45 + F.floor(r(66) * 240)).cast("int")))
        .withColumn("unit_cost_contract", F.round(F.col("unit_cost") * (0.95 + r(67) * 0.08), 2))
        .select("po_id", "sku_id", "ordered_units", "unit_cost_contract", "category", "short_rate", "shock_flag")
    )

    # ----------------------------
    # FACT: Receipts + Receipt Lines (received vs ordered, defects)
    # ----------------------------
    fact_receipt = (
        fact_po.filter(F.col("status") == "RECEIVED")
        .select("po_id", "dc_id", "received_date", "promised_date", "supplier_id", "shock_flag")
        .withColumn("receipt_id", F.concat(F.lit("RCPT-"), F.col("po_id")))
    )

    # Received units reflect short-ship probability (worse during shock)
    fact_receipt_line = (
        fact_receipt.select("receipt_id", "po_id", "dc_id", "received_date", "promised_date", "supplier_id", "shock_flag")
        .join(fact_po_line.select("po_id", "sku_id", "ordered_units", "category", "short_rate"), "po_id", "left")
        .join(dim_sku.select("sku_id", "is_perishable"), "sku_id", "left")
        .withColumn("effective_short_rate",
                    F.col("short_rate") * F.when(F.col("shock_flag") == 1, F.lit(1.6)).otherwise(F.lit(1.0)))
        .withColumn("short_factor", F.when(r(70) < F.col("effective_short_rate"), (0.65 + r(71) * 0.30)).otherwise(F.lit(1.0)))
        .withColumn("received_units", F.floor(F.col("ordered_units") * F.col("short_factor")).cast("int"))
        # defects higher on perishables; higher during shocks
        .withColumn("defect_rate",
                    F.when(F.col("is_perishable"), F.lit(0.015)).otherwise(F.lit(0.004)) *
                    F.when(F.col("shock_flag") == 1, F.lit(1.7)).otherwise(F.lit(1.0)))
        .withColumn("defect_units", F.floor(F.col("received_units") * F.col("defect_rate") * (0.6 + r(72) * 1.2)).cast("int"))
        .select("receipt_id", "po_id", "dc_id", "supplier_id", "received_date", "promised_date",
                "sku_id", "ordered_units", "received_units", "defect_units", "category", "shock_flag")
    )

    # ----------------------------
    # FACT: Shipments (DC -> Store)
    # ----------------------------
    # Create shipments daily per store (one shipment per store-day)
    fact_shipment = (
        dim_store.select("store_id", "dc_id", "region")
        .crossJoin(dim_date.select("date", "shock_flag", "delay_multiplier"))
        .withColumn("shipment_id", F.concat(F.lit("SH-"), F.date_format("date", "yyyyMMdd"), F.lit("-"), F.col("store_id")))
        .withColumn("carrier", F.when(r(80) < 0.45, F.lit("Carrier-A"))
                              .when(r(81) < 0.75, F.lit("Carrier-B"))
                              .otherwise(F.lit("Carrier-C")))
        .withColumn("mode", F.when(r(82) < 0.85, F.lit("TRUCK")).otherwise(F.lit("LTL")))
        .withColumn("ship_date", F.col("date"))
        # delivery delay: usually next day; worse during shocks
        .withColumn("delivery_delay_days",
                    F.when(r(83) < (0.10 * F.col("delay_multiplier")), F.lit(2)).otherwise(F.lit(1)))
        .withColumn("delivery_date", F.expr("date_add(ship_date, delivery_delay_days)"))
        # cost correlates with mode + shock + random
        .withColumn("base_cost",
                    F.when(F.col("mode") == "TRUCK", F.lit(260.0)).otherwise(F.lit(160.0)))
        .withColumn("cost",
                    F.round(F.col("base_cost") * (0.85 + r(84) * 0.6) * F.when(F.col("shock_flag") == 1, F.lit(1.25)).otherwise(F.lit(1.0)), 2))
        .select("shipment_id", "dc_id", "store_id", "region", "carrier", "mode", "ship_date", "delivery_date", "cost", "shock_flag")
    )

    return {
        # dims
        "dim_date": dim_date,
        "dim_sku": dim_sku,
        "dim_store": dim_store,
        "dim_dc": dim_dc,
        "dim_supplier": dim_supplier,
        # facts
        "fact_sales_daily": fact_sales_daily,
        "fact_inventory_daily": fact_inventory_daily,
        "fact_po": fact_po,
        "fact_po_line": fact_po_line,
        "fact_receipt": fact_receipt,
        "fact_receipt_line": fact_receipt_line,
        "fact_shipment": fact_shipment,
    }

# Example usage:
# cfg = SupplyChainSynthConfig(days=180, sales_sparsity=0.35)
# dfs = generate_supply_chain_data(spark, cfg)
# for name, df in dfs.items():
#     df.write.mode("overwrite").format("delta").saveAsTable(f"demo_supply_chain.{name}")