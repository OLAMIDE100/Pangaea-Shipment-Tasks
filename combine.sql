CREATE TABLE Shipment_Records.Shipment_database
PARTITION BY shipment_datetime
CLUSTER BY carrier
 AS
SELECT * 

FROM(
    SELECT * 
    FROM (SELECT *
            FROM (SELECT * 
                    FROM (SELECT address_id,id AS shipment_id,datetime AS shipment_datetime,FORMAT_DATE('%B', datetime) AS month
                            FROM `endless-context-338620.Shipment_Records.Shipments`
                        ) AS shipment
                    LEFT JOIN  (SELECT address_id,countryname,address.country_code
                            FROM 
                                (SELECT id AS address_id,country AS country_code
                                    FROM `endless-context-338620.Shipment_Records.Address`
                                ) AS address
                            LEFT JOIN
                                (SELECT countryname,code AS country_code
                                FROM `endless-context-338620.Shipment_Records.Country`
                                ) AS country
                            USING(country_code)
                            ) AS addresss  
                    USING(address_id)
                ) AS result
            LEFT JOIN (SELECT id AS tracker_id,datetime AS tracked_datetime,shipment_id,carrier
                    FROM `endless-context-338620.Shipment_Records.Trackers`
                ) AS tracker
            USING(Shipment_id)
        ) AS result_v2

    FULL OUTER JOIN (SELECT id AS item_id,datetime AS current_status_datetime,status,tracker_id
            FROM `endless-context-338620.Shipment_Records.Details`
        ) AS item
    USING(tracker_id)
    ) AS combine_table


