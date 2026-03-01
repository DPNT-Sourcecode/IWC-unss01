from solutions.SUM.sum_solution import SumSolution


class TestSum():
    def test_sum(self):
        assert SumSolution().compute(0, 0) == 0
        assert SumSolution().compute(100, 100) == 200
        assert SumSolution().compute(0, 100) == 100
        assert SumSolution().compute(100, 0) == 100
        assert SumSolution().compute(1, 1) == 2
        assert SumSolution().compute(-1, -1) == -2


