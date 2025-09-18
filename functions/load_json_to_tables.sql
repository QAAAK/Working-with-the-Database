-- DROP FUNCTION public.load_json_to_tables(jsonb);

CREATE OR REPLACE FUNCTION public.load_json_to_tables(json_array jsonb)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
DECLARE
    json_element jsonb;
    item jsonb;
    offer jsonb;
    result_text text := 'PASSED';
BEGIN

	/* 
	Циклом обходим элементы массива (в массиве будет несколько json пакетов) и загружаем первую часть orders
	*/
	
    FOR json_element IN SELECT * FROM jsonb_array_elements(json_array)
    LOOP
        INSERT INTO orders(
            id,
            type,
            time,
            order_id,
            realm_id,
            request_id,
            origin_user_phone,
            customer_phone,
            order_date,
            complete_date,
            total_amount,
            total_currency,
            state,
            ep_checkout_id,
            ep_product_order_id
        )
        VALUES (
            (json_element ->> 'id')::uuid,
            json_element ->> 'type',
            (json_element ->> 'time')::timestamp,
            (json_element -> 'data' -> 'order' ->> 'orderId'),
            (json_element -> 'data' -> 'order' ->> 'realmId'),
            (json_element -> 'data' -> 'order' ->> 'requestId'),
            (json_element -> 'data' -> 'order' -> 'originUser' ->> 'phone'),
            (json_element -> 'data' -> 'order' -> 'customer' ->> 'phone'),
            (json_element -> 'data' -> 'order' ->> 'orderDate')::timestamp,
            (json_element -> 'data' -> 'order' ->> 'completeDate')::timestamp,
            (json_element -> 'data' -> 'order' -> 'total' ->> 'amount')::int,
            (json_element -> 'data' -> 'order' -> 'total' ->> 'currency'),
            (json_element -> 'data' -> 'order' ->> 'state'),
            (json_element -> 'data' -> 'order' -> 'epCheckout' ->> 'id'),
            (json_element -> 'data' -> 'order' -> 'epProductOrder' ->> 'id')
        );

		/* 
		Два вложенных цикла загружают две вложенные конструкции json-пакета
		*/

        FOR item IN SELECT * FROM jsonb_array_elements(json_element -> 'data' -> 'order' -> 'items')
        LOOP
            INSERT INTO items(
                order_id,
                product_id,
                name,
                description,
                result
            )
            VALUES (
                (json_element ->> 'id')::uuid,
                (item ->> 'productId')::uuid,
                item ->> 'name',
                item ->> 'description',
                item ->> 'result'
            );
        END LOOP;

        FOR offer IN SELECT * FROM jsonb_array_elements(json_element -> 'data' -> 'order' -> 'epProductOffers')
        LOOP
            INSERT INTO ep_product_offers(
                order_id,
                offer_id
            )
            VALUES (
                (json_element ->> 'id')::uuid,
                offer ->> 'id'
            );
        END LOOP;
    END LOOP;
END;

$$
EXECUTE ON ANY;

