from graphlab import SFrame, SGraph, shortest_path

url = '/home/gengl/Datasets/SSSP/BerkStan/edge.txt'
data = SFrame.read_csv(url, delimiter='\t', header=False, column_type_hints=[int, int, int])
graph = SGraph().add_edges(data, src_field='X1', dst_field='X2')
sp_model = shortest_path.create(graph, source_vid=0, weight_field='X3')
sp_model.summary()
