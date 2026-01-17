from solutions.HLO.hello_solution import HelloSolution


class TestHello():
    def test_hello(self):
        assert HelloSolution().hello("There") == "Hello, There!"
        assert HelloSolution().hello("Roger") == "Hello, Roger!"