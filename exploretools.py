from collections import defaultdict
import pandas as pd


class Explorer(object):

	def __init__(self, es):
		self._es = es


	def value_counts_exact(self, docs_param, field):
		"""
		This iteratively retrieves all values for a field and 
		uses python dict to compute value counts.
		"""
		page = self._es.search(
			**docs_param,
			scroll='2m',
			size=1000,
			body={"query": {"match_all": {}}})
		sid = page['_scroll_id']
		scroll_size = len(page['hits']['hits'])

		value_counts = defaultdict(int)
		n_entries = 0
		while(scroll_size > 0):
			
			n_entries += scroll_size
			for entry in page['hits']['hits']:
				val = entry['_source'][field]
				value_counts[val] += 1
			
			page = self._es.scroll(scroll_id=sid, scroll='2m')
			sid = page['_scroll_id']
			scroll_size = len(page['hits']['hits'])
				
		print(f"Went through {n_entries} entries.")
		print(f"Found {len(value_counts)} unique entries.")
		return pd.Series(value_counts).sort_values(ascending=False)
		

	def n_unique_exact(self, docs_param, field):
		return len(self.value_counts_exact(docs_param, field))
		

	def unique_exact(self, docs_param, field):
		return list(self.value_counts_exact(docs_param, field).index)
		
		
	def n_unique_approx(self, docs_param, field):
		"""
		This is elastic native way how to count distinct values
		but it is only approximate.
		"""
		body = {'size': 0,
				'aggs': {'distinct_ids': {'cardinality': {'field': field}}}}
		return self._es.search(**docs_param, body=body)['aggregations']['distinct_ids']['value']