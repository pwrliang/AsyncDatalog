from graphlab import SFrame, SGraph, pagerank

url = '/home/gengl/Datasets/PageRank/BerkStan/edge.txt'
data = SFrame.read_csv(url, delimiter='\t', header=False, column_type_hints=[int, int])
graph = SGraph().add_edges(data, src_field='X1', dst_field='X2')
pr_model = pagerank.create(graph, reset_probability=0.2, threshold=0.0001, max_iterations=1000)
pr_model.summary()
