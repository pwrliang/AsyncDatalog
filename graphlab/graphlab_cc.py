from graphlab import SFrame, SGraph, connected_components

url = '/home/gengl/Datasets/CC/BerkStan/edge.txt'
data = SFrame.read_csv(url, delimiter='\t', header=False, column_type_hints=[int, int])
graph = SGraph().add_edges(data, src_field='X1', dst_field='X2')
cc_model = connected_components.create(graph, verbose=True)
cc_model.summary()
