"""Order payload construction and persistence helpers for source DB generation."""

import random
from datetime import datetime
from typing import Callable

import numpy as np
from psycopg2.extras import execute_values

INSERT_ORDERS_PAGE_SIZE = 10_000


def build_order_payload(
    *,
    catalogs,
    cust_info,
    num_orders: int,
    is_seed: bool,
    params,
    logger,
    build_calendar_month_probabilities: Callable[[], np.ndarray],
    sample_order_date_with_temporal_pattern: Callable[..., datetime],
    sample_items_per_order: Callable[[], int],
    sample_item_quantity: Callable[[], int],
):
    """Build order and order-item payloads from current catalog/customer state."""
    order_list, items_by_order = [], []
    now_ts = datetime.now()
    calendar_month_probs = build_calendar_month_probabilities()
    valid_customers = []
    # Step 1: keep only customers whose company currently has a purchasable catalog.
    for cid, meta in cust_info.items():
        cat = catalogs.get(meta["cuit"], [])
        if not cat:
            continue
        created_at = meta["created_at"] if meta["created_at"] is not None else datetime.min
        valid_customers.append({"id": cid, "cuit": meta["cuit"], "created_at": created_at})

    if not valid_customers:
        logger.warning("BATCH: No eligible customers with catalogs found. Skipping order generation.")
        return order_list, items_by_order

    # Step 2: pre-sample and sort order dates so customer eligibility respects lifecycle timing.
    valid_customers.sort(key=lambda x: x["created_at"])
    sampled_order_dates = [
        sample_order_date_with_temporal_pattern(
            customer_created_at=None,
            now_ts=now_ts,
            month_probs=calendar_month_probs,
            is_seed=is_seed,
        )
        for _ in range(num_orders)
    ]
    sampled_order_dates.sort()

    customer_cursor = 0
    eligible_customers = []
    # Step 3: for each sampled date, pick an eligible customer and synthesize line items.
    for order_date in sampled_order_dates:
        while customer_cursor < len(valid_customers) and valid_customers[customer_cursor]["created_at"] <= order_date:
            eligible_customers.append(valid_customers[customer_cursor])
            customer_cursor += 1
        if not eligible_customers:
            continue

        chosen_customer = random.choice(eligible_customers)
        customer_id = chosen_customer["id"]
        customer_cuit = chosen_customer["cuit"]
        cat = catalogs[customer_cuit]

        status = random.choices(
            ["COMPLETED", "CANCELLED", "RETURNED" if is_seed else "COMPLETED"],
            weights=params.seed_status_weights if is_seed else [0.97, 0.03, 0],
        )[0]

        num_items = sample_items_per_order()
        selected = np.random.choice(len(cat), size=min(num_items, len(cat)), replace=False)
        total, current_items = 0, []
        for idx in selected:
            product_id, price = cat[idx]
            qty = sample_item_quantity()
            total += qty * price
            current_items.append({"product_id": product_id, "quantity": qty, "unit_price": price})

        order_list.append(
            {
                "company_cuit": customer_cuit,
                "customer_id": customer_id,
                "status": status,
                "order_date": order_date,
                "total_amount": round(total, 2),
                "created_at": order_date,
                # Keep seed rows historical, but stamp incremental inserts as "now"
                # so incremental_timestamp extraction does not miss backdated orders.
                "updated_at": order_date if is_seed else now_ts,
            }
        )
        items_by_order.append(current_items)

    if len(order_list) < num_orders:
        logger.warning(
            f"BATCH: Generated {len(order_list)} of {num_orders} requested orders due to customer eligibility by date."
        )
    return order_list, items_by_order


def persist_orders_and_items(
    *,
    conn,
    order_list,
    items_by_order,
    pg_bulk_copy_rows: Callable[..., None],
):
    """Persist generated orders and items and return inserted counts."""
    if not order_list:
        return 0, 0

    # Insert orders and their items in one transaction so partial writes cannot occur.
    try:
        # Insert and capture IDs from this exact statement, avoiding race-prone "latest N ids" lookups.
        order_columns = [
            "company_cuit",
            "customer_id",
            "status",
            "order_date",
            "total_amount",
            "created_at",
            "updated_at",
        ]
        order_values = [tuple(order[column] for column in order_columns) for order in order_list]
        insert_sql = """
            INSERT INTO orders (company_cuit, customer_id, status, order_date, total_amount, created_at, updated_at)
            VALUES %s
            RETURNING id
        """
        inserted_order_ids: list[int] = []
        with conn.cursor() as cur:
            for start in range(0, len(order_values), INSERT_ORDERS_PAGE_SIZE):
                batch = order_values[start : start + INSERT_ORDERS_PAGE_SIZE]
                returned_rows = execute_values(cur, insert_sql, batch, page_size=len(batch), fetch=True)
                inserted_order_ids.extend([row[0] for row in returned_rows])

        final_items = []
        for idx, order_id in enumerate(inserted_order_ids):
            for item in items_by_order[idx]:
                item["order_id"] = order_id
                final_items.append(item)
        if final_items:
            pg_bulk_copy_rows(conn, final_items, "order_items")
        else:
            conn.commit()
        return len(order_list), len(final_items)
    except Exception:
        conn.rollback()
        raise
