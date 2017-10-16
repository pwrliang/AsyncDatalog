import collections
import random
import sys
import re
from myria.relation import MyriaRelation
from myria import MyriaQuery
from raco.algebra import *
from raco.expression import NamedAttributeRef as AttRef
from raco.expression import UnnamedAttributeRef as AttIndex
from raco.expression import StateVar
from raco.expression import aggregate

from raco.backends.myria import (
    MyriaShuffleConsumer, MyriaShuffleProducer, MyriaHyperCubeShuffleProducer,
    MyriaBroadcastConsumer, MyriaQueryScan, MyriaSplitConsumer, MyriaUnionAll,
    MyriaBroadcastProducer, MyriaScan, MyriaSelect, MyriaSplitProducer,
    MyriaDupElim, MyriaGroupBy, MyriaIDBController, MyriaSymmetricHashJoin,
    compile_to_json)
from raco.backends.myria import (MyriaLeftDeepTreeAlgebra,
                                 MyriaHyperCubeAlgebra)
from raco.compile import optimize
from raco import relation_key
from raco.catalog import FakeCatalog

import raco.scheme as scheme
import raco.myrial.myrial_test as myrial_test
from raco import types


class OptimizerTest(myrial_test.MyrialTestCase):
    x_scheme = scheme.Scheme([("a", types.LONG_TYPE), ("b", types.LONG_TYPE), ("c", types.LONG_TYPE)])  # noqa
    y_scheme = scheme.Scheme([("d", types.LONG_TYPE), ("e", types.LONG_TYPE), ("f", types.LONG_TYPE)])  # noqa
    z_scheme = scheme.Scheme([('src', types.LONG_TYPE), ('dst', types.LONG_TYPE)])  # noqa
    part_scheme = scheme.Scheme([("g", types.LONG_TYPE), ("h", types.LONG_TYPE), ("i", types.LONG_TYPE)])  # noqa
    broad_scheme = scheme.Scheme([("j", types.LONG_TYPE), ("k", types.LONG_TYPE), ("l", types.LONG_TYPE)])  # noqa
    x_key = relation_key.RelationKey.from_string("public:adhoc:X")
    y_key = relation_key.RelationKey.from_string("public:adhoc:Y")
    z_key = relation_key.RelationKey.from_string("public:adhoc:Z")
    part_key = relation_key.RelationKey.from_string("public:adhoc:part")
    broad_key = relation_key.RelationKey.from_string("public:adhoc:broad")
    part_partition = RepresentationProperties(
        hash_partitioned=tuple([AttIndex(1)]))
    broad_partition = RepresentationProperties(broadcasted=True)
    random.seed(387)  # make results deterministic
    rng = 20
    count = 30
    z_data = collections.Counter([(1, 2), (2, 3), (1, 2), (3, 4)])
    x_data = collections.Counter(
        [(random.randrange(rng), random.randrange(rng),
          random.randrange(rng)) for _ in range(count)])
    y_data = collections.Counter(
        [(random.randrange(rng), random.randrange(rng),
          random.randrange(rng)) for _ in range(count)])
    part_data = collections.Counter(
        [(random.randrange(rng), random.randrange(rng),
          random.randrange(rng)) for _ in range(count)])
    broad_data = collections.Counter(
        [(random.randrange(rng), random.randrange(rng),
          random.randrange(rng)) for _ in range(count)])

    def setUp(self):
        super(OptimizerTest, self).setUp()
        self.db.ingest(self.x_key, self.x_data, self.x_scheme)
        self.db.ingest(self.y_key, self.y_data, self.y_scheme)
        self.db.ingest(self.z_key, self.z_data, self.z_scheme)
        self.db.ingest(self.part_key, self.part_data, self.part_scheme,
                       self.part_partition)  # "partitioned" table
        self.db.ingest(self.broad_key, self.broad_data,
                       self.broad_scheme, self.broad_partition)

    @staticmethod
    def logical_to_physical(lp, **kwargs):
        if kwargs.get('hypercube', False):
            algebra = MyriaHyperCubeAlgebra(FakeCatalog(64))
        else:
            algebra = MyriaLeftDeepTreeAlgebra()
        return optimize(lp, algebra, **kwargs)

    @staticmethod
    def get_count(op, claz):
        """Return the count of operator instances within an operator tree."""

        def count(_op):
            if isinstance(_op, claz):
                yield 1
            else:
                yield 0

        return sum(op.postorder(count))

    @staticmethod
    def get_num_select_conjuncs(op):
        """Get the number of conjunctions within all select operations."""

        def count(_op):
            if isinstance(_op, Select):
                yield len(expression.extract_conjuncs(_op.condition))
            else:
                yield 0

        return sum(op.postorder(count))

    def list_ops_in_json(self, plan, type):
        ops = []
        for p in plan['plan']['plans']:
            for frag in p['fragments']:
                for op in frag['operators']:
                    if op['opType'] == type:
                        ops.append(op)
        return ops

    def test_cc(self):
        """Test Connected Components"""
        path = "hdfs://master:9000/Datasets/CC/BerkStan/edge.txt"
        query = """
        E = load("file://%s", csv(schema(src:int, dst:int), skip=0));
        V = select distinct E.src as x from E;
        do
            CC = [nid, MIN(cid) as cid] <-
                 [from V emit V.x as nid, V.x as cid] +
                 [from E, CC where E.src = CC.nid emit E.dst as nid, CC.cid];
        until convergence pull_idb;
        store(CC, CC);
        """ % path
        lp = self.get_logical_plan(query, async_ft='REJOIN')
        pp = self.logical_to_physical(lp, async_ft='REJOIN')
        for op in pp.children():
            for child in op.children():
                if isinstance(child, MyriaIDBController):
                    # for checking rule RemoveSingleSplit
                    assert not isinstance(op, MyriaSplitProducer)
        plan = compile_to_json(query, lp, pp, 'datalog', async_ft='REJOIN')
        print plan
        # joins = [op for op in pp.walk()
        #          if isinstance(op, MyriaSymmetricHashJoin)]
        # query = MyriaQuery.submit(query, language="myrial")

        # assert len(joins) == 1
        # assert joins[0].pull_order_policy == 'RIGHT'
        # self.assertEquals(plan['ftMode'], 'REJOIN')
        idbs = self.list_ops_in_json(plan, 'IDBController')
        # self.assertEquals(len(idbs), 1)
        # self.assertEquals(idbs[0]['argState']['type'], 'KeepMinValue')
        # self.assertEquals(idbs[0]['sync'], False)  # default value: async
        # sps = self.list_ops_in_json(plan, 'ShuffleProducer')
        # assert any(sp['argBufferStateType']['type'] == 'KeepMinValue'
        #            for sp in sps if 'argBufferStateType' in sp and
        #            sp['argBufferStateType'] is not None)

    def test_lca(self):
        """Test LCA"""
        query = """
        Cite = scan(public:adhoc:X);
        Paper = scan(public:adhoc:Y);
        do
        Ancestor = [a,b,MIN(dis) as dis] <- [from Cite emit a, b, 1 as dis] +
                [from Ancestor, Cite
                 where Ancestor.b = Cite.a
                 emit Ancestor.a, Cite.b, Ancestor.dis+1];
        LCA = [pid1,pid2,LEXMIN(dis,yr,anc)] <-
                [from Ancestor as A1, Ancestor as A2, Paper
                 where A1.b = A2.b and A1.b = Paper.d and A1.a < A2.a
                 emit A1.a as pid1, A2.a as pid2,
                 greater(A1.dis, A2.dis) as dis,
                 Paper.e as yr, A1.b as anc];
        until convergence sync;
        store(LCA, LCA);
        """
        lp = self.get_logical_plan(query, async_ft='REJOIN')
        pp = self.logical_to_physical(lp, async_ft='REJOIN')
        plan = compile_to_json(query, lp, pp, 'myrial', async_ft='REJOIN')
        idbs = self.list_ops_in_json(plan, 'IDBController')
        self.assertEquals(len(idbs), 2)
        self.assertEquals(idbs[0]['argState']['type'], 'KeepMinValue')
        self.assertEquals(idbs[1]['argState']['type'], 'KeepMinValue')
        self.assertEquals(len(idbs[1]['argState']['valueColIndices']), 3)
        self.assertEquals(idbs[0]['sync'], True)
        self.assertEquals(idbs[1]['sync'], True)

    def test_galaxy_evolution(self):
        """Test Galaxy Evolution"""
        query = """
        GoI = scan(public:adhoc:X);
        Particles = scan(public:adhoc:Y);
        do
        Edges = [time,gid1,gid2,COUNT(*) as num] <-
                [from Particles as P1, Particles as P2, Galaxies
                where P1.d = P2.d and P1.f+1 = P2.f and
                      P1.f = Galaxies.time and Galaxies.gid = P1.e
                emit P1.f as time, P1.e as gid1, P2.e as gid2];
        Galaxies = [time, gid] <-
          [from GoI emit 1 as time, GoI.a as gid] +
          [from Galaxies, Edges
           where Galaxies.time = Edges.time and
           Galaxies.gid = Edges.gid1 and Edges.num >= 4
           emit Galaxies.time+1, Edges.gid2 as gid];
        until convergence async build_EDB;
        store(Galaxies, Galaxies);
        """
        lp = self.get_logical_plan(query, async_ft='REJOIN')
        for op in lp.walk():
            if isinstance(op, Select):
                # for checking rule RemoveEmptyFilter
                assert (op.condition is not None)
        pp = self.logical_to_physical(lp, async_ft='REJOIN')
        plan = compile_to_json(query, lp, pp, 'myrial', async_ft='REJOIN')
        joins = [op for op in pp.walk()
                 if isinstance(op, MyriaSymmetricHashJoin)]
        # The two joins for Edges
        assert len(
            [j for j in joins if j.pull_order_policy == 'LEFT_EOS']) == 2

        idbs = self.list_ops_in_json(plan, 'IDBController')
        self.assertEquals(len(idbs), 2)
        self.assertEquals(idbs[0]['argState']['type'], 'CountFilter')
        self.assertEquals(idbs[1]['argState']['type'], 'DupElim')
        self.assertEquals(idbs[0]['sync'], False)
        self.assertEquals(idbs[1]['sync'], False)

        super(OptimizerTest, self).new_processor()
        query = """
        GoI = scan(public:adhoc:X);
        Particles = scan(public:adhoc:Y);
        do
        Edges = [time,gid1,gid2,COUNT(*) as num] <-
                [from Particles as P1, Particles as P2, Galaxies
                where P1.d = P2.d and P1.f+1 = P2.f and
                      P1.f = Galaxies.time and Galaxies.gid = P1.e
                emit P1.f as time, P1.e as gid1, P2.e as gid2];
        Galaxies = [time, gid] <-
          [from GoI emit 1 as time, GoI.a as gid] +
          [from Galaxies, Edges
           where Galaxies.time = Edges.time and
           Galaxies.gid = Edges.gid1 and Edges.num > 3
           emit Galaxies.time+1, Edges.gid2 as gid];
        until convergence async build_EDB;
        store(Galaxies, Galaxies);
        """
        lp = self.get_logical_plan(query, async_ft='REJOIN')
        pp = self.logical_to_physical(lp, async_ft='REJOIN')
        plan_gt = compile_to_json(query, lp, pp, 'myrial', async_ft='REJOIN')
        idbs_gt = self.list_ops_in_json(plan_gt, 'IDBController')
        self.assertEquals(idbs_gt[0], idbs[0])

    def test_push_select_below_shuffle(self):
        """Test pushing selections below shuffles."""
        lp = StoreTemp('OUTPUT',
                       Select(expression.LTEQ(AttRef("a"), AttRef("b")),
                              Shuffle(
                                  Scan(self.x_key, self.x_scheme),
                                  [AttRef("a"), AttRef("b")], 'Hash')))  # noqa

        self.assertEquals(self.get_count(lp, StoreTemp), 1)
        self.assertEquals(self.get_count(lp, Select), 1)
        self.assertEquals(self.get_count(lp, Shuffle), 1)
        self.assertEquals(self.get_count(lp, Scan), 1)

        pp = self.logical_to_physical(lp)
        self.assertIsInstance(pp.input, MyriaShuffleConsumer)
        self.assertIsInstance(pp.input.input, MyriaShuffleProducer)
        self.assertIsInstance(pp.input.input.input, Select)
        self.assertIsInstance(pp.input.input.input.input, Scan)


if __name__ == '__main__':
    # test = OptimizerTest()
    # test.test_cc()
    query = """
    E = load("file://%s", csv(schema(src:int, dst:int), skip=0));
    
    """ % "hdfs://master:9000/Datasets/CC/BerkStan/edge.txt"
    MyriaQuery.submit(query, language="datalog")
