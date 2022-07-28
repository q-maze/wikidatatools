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


def get_small_subset(subject, return_pd_df=True, output_path=None):
    """
    Args:
        subject: Subject to retrieve all instances of/instaces of subclasses of
        return_pd_df: Return a pandas dataframe object with query output
        output_path: Path to write .tsv file output to

    Returns:
        return_df: Returned if return_pd_df = True, dataframe containing
        elements from SPAQRL query.
    """
    url = "https://query.wikidata.org/sparql"
    endpoint = SPARQLWrapper(url)
    query = """
            SELECT DISTINCT ?subject ?predicate ?object
            WHERE
            {
              ?subject ?predicate ?object. # Where there is a complete triple
              wd:""" + subject + """ ^wdt:P279*/^wdt:P31 ?subject. 
              wd:""" + subject + """ ^wdt:P279*/^wdt:P31 ?object.
            }
            """
    endpoint.setQuery(query)
    endpoint.setReturnFormat(JSON)
    json_result = endpoint.queryAndConvert()
    return_df = pd.json_normalize(json_result["results"]["bindings"])
    return_df = return_df[[col for col in return_df.columns if ".value" in col]]
    return_df.columns = [col.split(".")[0] for col in return_df.columns]
    for col in list(return_df.columns):
        return_df[col] = return_df[col].str.split('/').str[-1]

    if output_path:
        return_df.to_csv(output_path, sep="\t")

    if return_pd_df:
        return return_df


def create_subset(targets, input_path, output_path):
    """ Creates a subset of Wikidata items which are instances of
    targets subclasses and saves it to output_path.
    Args:
        targets: List of target subclasses.
        input_path: Filepath to processed Truthy file.
        output_path: Output filepath to dump subset parquet file.

    """
    target_classes = get_all_subclasses(targets)
    edges_df = dd.read_csv(input_path, sep="\n", header=None, names=["Subject", "Predicate", "Object"])
    instances = edges_df[(edges_df['Predicate'] == 'P31') & (edges_df['Object'].isin(target_classes))]
    instance_list = list(instances.compute())
    df = edges_df[(edges_df['Subject'].isin(instance_list)) & (edges_df['Object'].isin(instance_list))]
    df.to_parquet(output_path + "output.parquet")
