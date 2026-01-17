from solutions.HLO.HelloSolution import HelloSolution


class TestHello():
    def test_hello(self):
        assert HelloSolution().hello("There") == "Hello, There!"