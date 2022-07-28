import dask.dataframe as dd
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

def get_all_subclasses(subjects):
    """Returns a list of all Wikidata subclasses of a given topic/topics

    Args:
        subjects: Iterable of strings containing Wikidata items to retrieve
        subclasses of.

    Returns:
        results: List of all Wikidata subclasses of a given topic/topics
    """
    url = "https://query.wikidata.org/sparql"
    endpoint = SPARQLWrapper(url)
    results = []
    for subject in subjects:
        query = """
        SELECT ?item
        WHERE {
            ?item wdt:P279* wd:""" + str(subject) + """ .
        }"""
        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)

        r = [item['item']['value'][31:] for item in endpoint.query().convert()['results']['bindings']]
        results = results + r
    return results


def create_subset(targets, input_path, output_path):
    """ Creates a subset of Wikidata items which are instances of
    targets subclasses and saves it to output_path.
    Args:
        targets: List of target subclasses.
        input_path: Filepath to processed Truthy file.
        output_path: Output filepath to dump subset parquet file.

    """
    print("Requesting subclasses...")
    target_classes = get_all_subclasses(targets)
    print("Subclasses received.")
    print("Creating dask df...")
    edges_df = dd.read_csv(input_path, header=0)
    print(edges_df.head())
    print("Dask df created.")
    print("Getting instances...")
    instances = edges_df[(edges_df['Entity_2'] == 'P31') & (edges_df['Entity_3'].isin(target_classes))]
    print(instances.head())
    print('Creating instance list...')
    instance_list = list(instances['Entity_1'].compute())
    print("Instances retrieved and listed.")
    print("Filtering df...")
    df = edges_df[(edges_df['Entity_1'].isin(instance_list)) & (edges_df['Entity_3'].isin(instance_list))]
    print(df.head())
    print("Dataframe filtered.")
    print("Writing parquet...")
    df.compute().dropna().to_csv(output_path + "edges.tsv", sep="\t", index=0, header=False)
    print("Complete.")
