

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from apache_beam.io.gcp.internal.clients import bigquery


# This Class Transforms the Customer full name into first name and last name
class Parse_Name(beam.DoFn):
    def process(self, element):
        first_name = " "
        last_name = " "
        data_dict = json.loads(element.decode('utf-8'))
        for k, v in data_dict.items():
            if k == "customer_name":
                first_name = v.split(" ")[0]
                last_name = v.split(" ")[1]
        data_dict["customer_firstname"] = first_name
        data_dict["customer_lastname"] = last_name
        yield data_dict


# This Class is for Parsing Address. There was no uniformity in the addresses. So used different if conditions to put
# some values in the columns.

class Parse_Address(beam.DoFn):
    def process(self, element):
        for k, v in element.items():
            if k == "order_address":
                add_dict = {}
                length = len(v)
                v_values = v.split(",")
                if len(v_values) == 3:
                    v_values_add1 = v_values[0].split(" ", 1)
                    building_no = v_values_add1[0]
                    address = v_values_add1[1]
                    city = v_values[1]
                    state_new = v_values[2].split(" ", 1)
                    state = state_new[1].split(" ", 1)
                    states = state[0]
                    zipcode = state[1]
                    add_dict["order_building_no"] = building_no
                    add_dict["order_street_name"] = address
                    add_dict["order_city"] = city
                    add_dict["order_state_code"] = states
                    add_dict["order_zip_code"] = zipcode
                elif len(v_values) == 2:

                    v_values_add1 = v_values[0].split(" ", 1)

                    if len(v_values_add1) == 3:
                        print("inside 3 of 2 ")
                        building_no = v_values_add1[0]
                        address = v_values_add1[1]
                        city = v_values[1]
                        state_new = v_values[2].split(" ", 2)
                        zip_2 = v[-length:-length + 7]
                        state = state_new[1]

                        add_dict["order_building_no"] = building_no
                        add_dict["order_street_name"] = address
                        add_dict["order_city"] = city
                        add_dict["order_state_code"] = state
                        add_dict["order_zip_code"] = zip_2
                    elif len(v_values_add1) == 2:

                        building_no = " "
                        address= v_values_add1[0]
                        city = " "
                        state = v_values_add1[1].split(" ", 2)[0]
                        zip_2 = v[-length:-length + 7]

                        add_dict["order_building_no"] = building_no
                        add_dict["order_street_name"] = address
                        add_dict["order_city"] = city
                        add_dict["order_state_code"] = state
                        add_dict["order_zip_code"] = zip_2
        element["order_address"] = [add_dict]
        yield element


# This Class is used to calculate the total cost

class Calculate_Amount(beam.DoFn):
    def process(self, element):
        total_sum = 0
        for k, v in element.items():
            if k == "order_items":
                for elements in v:
                    for k1, v1 in elements.items():
                        if k1 == "price":
                            total_sum += v1

        if k == "cost_shipping" or k == "cost_tax":
            total_sum += v
        element["cost_total"] = round(total_sum, 2)

        yield element

# This Clas filters just the required columns for the pub/sub data.
class Filter_order(beam.DoFn):
    def process(self, element):
        columns_to_extract = ["order_id", "order_items"]
        new_set = {k: element[k] for k in columns_to_extract}
        yield new_set

# Extracts just the item_id from the order_items, name and price are rejected.
class Order_Seg(beam.DoFn):
    def process(self, element):
        list_id = []
        for k, v in element.items():
            if k == "order_items":
                for item in v:
                    if isinstance(item, dict):
                        for k1, v1 in item.items():
                            if k1 == "id":
                                list_id.append(v1)
        element["items_list"] = list_id
        del element["order_items"]
        yield element


# Extracts just the USD Rows
def IS_USD(element1):
    return element1["order_currency"] == "USD"

# Extracts just the EUR Rows
def IS_EUR(element2):
    return element2["order_currency"] == "EUR"

# Extracts just the GBP Rows
def IS_GBP(element3):
    return element3["order_currency"] == "GBP"

# Extracts just the required Columns to put the result in the Bigquery tables
class filter_data(beam.DoFn):
    def process(self, data):
        columns_to_extract = ["order_id", "order_address", "customer_firstname", "customer_lastname", "customer_ip",
                              "cost_total"]
        new_set = {k: data[k] for k in columns_to_extract}
        yield new_set


if __name__ == '__main__':
    # Schema definition for all the three tables

    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'nullable'},
            {"name": "order_address", "type": "RECORD", 'mode': 'Repeated',
             'fields': [
                 {"name": "order_building_no", "type": "STRING", 'mode': 'Nullable'},
                 {"name": "order_street_name", "type": "STRING", 'mode': "Nullable"},
                 {"name": "order_city", "type": "STRING", 'mode': 'NULLABLE'},
                 {"name": "order_state_code", "type": "STRING", 'mode': 'NULLABLE'},
                 {"name": "order_zip_code", "type": "STRING", 'mode': 'NULLABLE'},
             ],
             },
            {"name": "customer_firstname", "type": "STRING", 'mode': 'NULLABLE'},
            {"name": "customer_lastname", "type": "STRING", 'mode': 'NULLABLE'},
            {'name': 'customer_ip', 'type': 'String', 'mode': 'nullable'},
            {"name": "cost_total", "type": "Float", 'mode': 'NULLABLE'}
        ]
    }
    # Table specifications for the first BigQuery Table

    table_spec1 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='n_mathialagan_proj_1',
        tableId='usd_order_payment_history')

    # Table specifications for the second  BigQuery Table

    table_spec2 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='n_mathialagan_proj_1',
        tableId='eur_order_payment_history')

    # Table specifications for the third Bigquery table

    table_spec3 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='n_mathialagan_proj_1',
        tableId='gbp_order_payment_history')

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Entire data being pulled from the Pub/Sub.

        entire_data = pipeline | beam.io.ReadFromPubSub(
            subscription="projects/york-cdf-start/subscriptions/dataflow-project-orders-sub")

        # Splitting the customer name into first and last name

        names = entire_data | beam.ParDo(Parse_Name())

        # Splitting the address

        address = names | beam.ParDo(Parse_Address())

        # Calculating total price by joining tax, shipping and price

        total_price = address | beam.ParDo(Calculate_Amount())

        # Filters and rearranges the order_items to write in the pub/sub

        order_separation = total_price | beam.ParDo(Filter_order())
        order_segregation = order_separation | beam.ParDo(Order_Seg())

        # Filters just the USD data

        dictionary_separation_1 = total_price | beam.Filter(IS_USD)

        # Filters just the EURO data

        dictionary_separation_2 = total_price | beam.Filter(IS_EUR)

        # Filters just the GBP data
        dictionary_separation_3 = total_price | beam.Filter(IS_GBP)

        # Filters just the required columns

        US_order = dictionary_separation_1 | "Filter_For_USD" >> beam.ParDo(filter_data())
        EU_order = dictionary_separation_2 | "Filter_For_EUR" >> beam.ParDo(filter_data())
        GBP_order = dictionary_separation_3 | "Filter_For_GBP" >> beam.ParDo(filter_data())

        # Writing to the first big query table

        US_order | "write1" >> beam.io.WriteToBigQuery(
            table_spec1,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # Writing to the first big query table

        EU_order | "write2" >> beam.io.WriteToBigQuery(
            table_spec2,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # Writing to the first big query table

        GBP_order | "write" >> beam.io.WriteToBigQuery(
            table_spec3,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # To convert the PCollection to bytecode, first I am converting the PCollection to dictionary and to string
        # and then encoding to byte codes.

        order_final = order_segregation | beam.Map(lambda s: json.dumps(s).encode("utf-8"))
        order_final | "Write to PubSub" >> beam.io.WriteToPubSub(topic="projects/york-cdf-start/topics/test",
                                                                 with_attributes=False)

        # Getting the data from the Pub/Sub and printing in the console.

        entire_data = pipeline | "Read Order_items" >> beam.io.ReadFromPubSub(
            topic="projects/york-cdf-start/topics/test") | beam.Map(print)
