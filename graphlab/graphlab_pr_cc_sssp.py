from graphlab import SFrame, SGraph, connected_components, pagerank, shortest_path
from graphlab import deploy


def PageRank():
    url = '/home/gengl/Datasets/PageRank/BerkStan/edge.txt'
    data = SFrame.read_csv(url, delimiter='\t', header=False, column_type_hints=[int, int])
    graph = SGraph().add_edges(data, src_field='X1', dst_field='X2')
    pr_model = pagerank.create(graph, reset_probability=0.2, threshold=0.0001, max_iterations=1000, _distributed=True)
    pr_model.summary()


def CC():
    url = '/home/gengl/Datasets/CC/BerkStan/edge.txt'
    data = SFrame.read_csv(url, delimiter='\t', header=False, column_type_hints=[int, int])
    graph = SGraph().add_edges(data, src_field='X1', dst_field='X2')
    cc_model = connected_components.create(graph, verbose=True)
    cc_model.summary()


def SSSP():
    url = '/home/gengl/Datasets/SSSP/BerkStan/edge.txt'
    data = SFrame.read_csv(url, delimiter='\t', header=False, column_type_hints=[int, int, int])
    graph = SGraph().add_edges(data, src_field='X1', dst_field='X2')
    sp_model = shortest_path.create(graph, source_vid=0, weight_field='X3')
    sp_model.summary()


# ec2config = deploy.Ec2Config(region='us-west-2',
#                              instance_type='m3.xlarge',
#                              aws_access_key_id='xxxx',
#                              aws_secret_access_key='xxxx')
# ec2 = deploy.ec2_cluster.create(name='ec2', s3_path='s3://bucket/path', ec2_config=ec2config)
#
# job_ec2 = deploy.job.create(PageRank, environment=ec2)
# job_ec2.get_results()
PageRank()