import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import SetupOptions
import json
import argparse


class Parse_Name(beam.DoFn):
    def process(self, element):
        first_name = " "
        last_name = " "
        data_dict = json.loads(element.decode('utf-8'))
        del data_dict["customer_time"]
        for k, v in data_dict.items():
            if k == "customer_name":
                first_name = v.split(" ")[0]
                last_name = v.split(" ")[1]
        data_dict["customer_firstname"] = first_name
        data_dict["customer_lastname"] = last_name
        yield data_dict


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
                        address = v_values_add1[0]
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


class Calculate_Amount(beam.DoFn):
    def process(self, element):
        total_sum = 0.00
        for k, v in element.items():
            if k == "order_items":
                for elements in v:
                    for k1, v1 in elements.items():
                        if k1 == "price":
                            total_sum += v1

        if k == "cost_shipping" or k == "cost_tax":
            total_sum += v
        element["cost_total"] = total_sum
        yield element


class Filter_order(beam.DoFn):
    def process(self, element):
        # data_dict_1 = json.loads(element.decode('utf-8'))
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


def IS_USD(element1):
    return element1["order_currency"] == "USD"


def IS_EUR(element2):
    return element2["order_currency"] == "EUR"


def IS_GBP(element3):
    return element3["order_currency"] == "GBP"


class filter_data(beam.DoFn):
    def process(self, data):
        columns_to_extract = ["order_id", "order_address", "customer_firstname", "customer_lastname", "customer_ip",
                              "cost_total"]
        new_set = {k: data[k] for k in columns_to_extract}
        yield new_set


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input"
    default = ""
    )
    parser.add_argument(
        "--output1",
        dest="output1",
        help="path of the first big query table"

    )
    parser.add_argument(
        "--output2",
        dest="output2",
        help="Path of the second big query table."
    )
    parser.add_argument(
        "--output3",
        dest="output3",
        help="path of the second big query table"
    )

    parser.add_argument(
        "--dataset",
        dest="dataset",
        help="Dataset name"
    )
    parser.add_argument(
        "--project",
        dest="project",
        help="Project name"
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    # pipeline_args.extend([
    #     '--project=' + known_args.project,
    #     '--input=' + known_args.input,
    #     '--output1=' + known_args.output1,
    #     '--output2=' + known_args.output2,
    #     '--output3=' + known_args.output3,
    #     '--dataset=' + known_args.dataset
    # ])

    pipeline_options1 = PipelineOptions(pipeline_args, streaming=True)
    pipeline_options1.view_as(SetupOptions).save_main_session = save_main_session
    # Set `save_main_session` to True so DoFns can access globally imported modules.
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
    table_spec1 = bigquery.TableReference(
        projectId=known_args.project,
        datasetId=known_args.dataset,
        tableId=known_args.output1)

    table_spec2 = bigquery.TableReference(
        projectId=known_args.project,
        datasetId=known_args.dataset,
        tableId=known_args.output2)

    table_spec3 = bigquery.TableReference(
        projectId=known_args.project,
        datasetId=known_args.dataset,
        tableId=known_args.output3)

    with beam.Pipeline(options=pipeline_options1) as pipeline:
        # Entire data being pulled from the Pub/Sub.

        entire_data = pipeline | "Reading for Pub/Sub" >> beam.io.ReadFromPubSub(subscription=known_args.input)

        # Splitting the customer name into first and last name

        names = entire_data | beam.ParDo(Parse_Name())

        # Splitting the address

        address = names | beam.ParDo(Parse_Address())

        # Calculating total price
        total_price = address | beam.ParDo(Calculate_Amount())

        order_separation = total_price | beam.ParDo(Filter_order())
        order_segregation = order_separation | beam.ParDo(Order_Seg())

        dictionary_seperation_1 = total_price | beam.Filter(IS_USD)
        dictionary_seperation_2 = total_price | beam.Filter(IS_EUR)
        dictionary_seperation_3 = total_price | beam.Filter(IS_GBP)

        US_order = dictionary_seperation_1 | "Filter_USD" >> beam.ParDo(filter_data())
        EU_order = dictionary_seperation_2 | "FILTER EURO" >> beam.ParDo(filter_data())
        GBP_order = dictionary_seperation_3 | "Filter GBP" >> beam.ParDo(filter_data())

        US_order | "write1" >> beam.io.WriteToBigQuery(
            table_spec1,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        EU_order | "write2" >> beam.io.WriteToBigQuery(
            table_spec2,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        GBP_order | "write3" >> beam.io.WriteToBigQuery(
            table_spec3,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        order_final = order_segregation | beam.Map(lambda s: json.dumps(s).encode("utf-8"))
        order_final | "Write to PubSub" >> beam.io.WriteToPubSub(
            topic="projects/york-cdf-start/topics/dataflow-order-stock-update",
            with_attributes=False)


if __name__ == '__main__':
    run()

