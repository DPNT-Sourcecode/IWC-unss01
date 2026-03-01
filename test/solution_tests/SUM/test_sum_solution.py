from solutions.SUM.sum_solution import SumSolution


class TestSum():
    def test_sum(self):
        assert SumSolution().compute(0, 0) == 0
        assert SumSolution().compute(1, 2) == 3

