from solutions.SUM.sum_solution import SumSolution


class TestSum():
    def test_sum_valid(self):
        sol = SumSolution()
        # Testing boundaries (0 and 100)
        assert sol.compute(0, 0) == 0
        assert sol.compute(100, 100) == 200
        assert sol.compute(0, 100) == 100
        assert sol.compute(100, 0) == 100
        # Testing middle values
        assert sol.compute(50, 25) == 75
        assert sol.compute(1, 1) == 2

    def test_sum_out_of_range(self):
        sol = SumSolution()
        # Testing ValueErrors (Below 0 or Above 100)
        try:
            sol.compute(-1, 50)
            assert False, "Should have raised ValueError for negative x"
        except ValueError:
            pass

        try:
            sol.compute(50, 101)
            assert False, "Should have raised ValueError for y > 100"
        except ValueError:
            pass

    def test_sum_wrong_types(self):
        sol = SumSolution()
        # Testing TypeErrors (Strings, Floats, None)
        try:
            sol.compute("10", 20)
            assert False, "Should have raised TypeError for string"
        except TypeError:
            pass

        try:
            sol.compute(10.5, 20)
            assert False, "Should have raised TypeError for float"
        except TypeError:
            pass



