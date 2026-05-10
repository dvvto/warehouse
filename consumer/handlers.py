import json
import logging

log = logging.getLogger(__name__)


def handle_product_received(db, event):
    product_id = event["product_id"]
    zone_id = event["zone_id"]
    quantity = event["quantity"]
    sku = event.get("sku") or ""

    avail, reserved = db.get_inventory(product_id, zone_id)
    db.update_inventory(product_id, zone_id, sku, avail + quantity, reserved)
    log.info(f"PRODUCT_RECEIVED: {product_id} zone={zone_id} +{quantity} -> avail={avail + quantity}")


def handle_product_shipped(db, event):
    product_id = event["product_id"]
    zone_id = event["zone_id"]
    quantity = event["quantity"]
    sku = event.get("sku") or ""

    avail, reserved = db.get_inventory(product_id, zone_id)
    new_avail = max(0, avail - quantity)
    db.update_inventory(product_id, zone_id, sku, new_avail, reserved)
    log.info(f"PRODUCT_SHIPPED: {product_id} zone={zone_id} -{quantity} -> avail={new_avail}")


def handle_product_moved(db, event):
    product_id = event["product_id"]
    src_zone = event["zone_id"]
    dst_zone = event["target_zone_id"]
    quantity = event["quantity"]
    sku = event.get("sku") or ""

    src_avail, src_reserved = db.get_inventory(product_id, src_zone)
    new_src = max(0, src_avail - quantity)
    db.update_inventory(product_id, src_zone, sku, new_src, src_reserved)

    dst_avail, dst_reserved = db.get_inventory(product_id, dst_zone)
    db.update_inventory(product_id, dst_zone, sku, dst_avail + quantity, dst_reserved)
    log.info(f"PRODUCT_MOVED: {product_id} {src_zone}->{dst_zone} qty={quantity}")


def handle_product_reserved(db, event):
    product_id = event["product_id"]
    zone_id = event["zone_id"]
    quantity = event["quantity"]
    sku = event.get("sku") or ""

    avail, reserved = db.get_inventory(product_id, zone_id)
    new_avail = max(0, avail - quantity)
    new_reserved = reserved + quantity
    db.update_inventory(product_id, zone_id, sku, new_avail, new_reserved)
    log.info(f"PRODUCT_RESERVED: {product_id} zone={zone_id} qty={quantity} -> avail={new_avail} reserved={new_reserved}")


def handle_product_released(db, event):
    product_id = event["product_id"]
    zone_id = event["zone_id"]
    quantity = event["quantity"]
    sku = event.get("sku") or ""

    avail, reserved = db.get_inventory(product_id, zone_id)
    new_reserved = max(0, reserved - quantity)
    new_avail = avail + quantity
    db.update_inventory(product_id, zone_id, sku, new_avail, new_reserved)
    log.info(f"PRODUCT_RELEASED: {product_id} zone={zone_id} qty={quantity} -> avail={new_avail} reserved={new_reserved}")


def handle_inventory_counted(db, event):
    product_id = event["product_id"]
    zone_id = event["zone_id"]
    counted = event["quantity"]
    sku = event.get("sku") or ""

    _, reserved = db.get_inventory(product_id, zone_id)
    db.update_inventory(product_id, zone_id, sku, counted, reserved)
    log.info(f"INVENTORY_COUNTED: {product_id} zone={zone_id} set avail={counted}")


def handle_order_created(db, event):
    order_id = event["order_id"]
    items = event.get("order_items") or []

    db.create_order(order_id, json.dumps(items))
    log.info(f"ORDER_CREATED: order_id={order_id} items={len(items)}")

    # Reserve each item
    for item in items:
        product_id = item["product_id"]
        zone_id = item["zone_id"]
        quantity = item["quantity"]
        avail, reserved = db.get_inventory(product_id, zone_id)
        new_avail = max(0, avail - quantity)
        db.update_inventory(product_id, zone_id, "", new_avail, reserved + quantity)
        log.info(f"  reserved {product_id} zone={zone_id} qty={quantity}")


def handle_order_completed(db, event):
    order_id = event["order_id"]
    items = event.get("order_items") or []

    db.update_order_status(order_id, "COMPLETED")
    log.info(f"ORDER_COMPLETED: order_id={order_id}")

    # Deduct reserved quantities (items are shipped)
    for item in items:
        product_id = item["product_id"]
        zone_id = item["zone_id"]
        quantity = item["quantity"]
        avail, reserved = db.get_inventory(product_id, zone_id)
        new_reserved = max(0, reserved - quantity)
        # available stays the same (already deducted at reservation)
        db.update_inventory(product_id, zone_id, "", avail, new_reserved)
        log.info(f"  shipped {product_id} zone={zone_id} qty={quantity} reserved={new_reserved}")


HANDLERS = {
    "PRODUCT_RECEIVED": handle_product_received,
    "PRODUCT_SHIPPED": handle_product_shipped,
    "PRODUCT_MOVED": handle_product_moved,
    "PRODUCT_RESERVED": handle_product_reserved,
    "PRODUCT_RELEASED": handle_product_released,
    "INVENTORY_COUNTED": handle_inventory_counted,
    "ORDER_CREATED": handle_order_created,
    "ORDER_COMPLETED": handle_order_completed,
}


def dispatch(db, event):
    event_type = event["event_type"]
    handler = HANDLERS.get(event_type)
    if handler is None:
        log.warning(f"No handler for event_type={event_type}")
        return
    handler(db, event)
