import unittest
from src.utils.singleton import Singleton


class TestSingleton(unittest.TestCase):
    def test_singleton_instance(self):
        class MyClass(metaclass=Singleton):
            pass

        instance1 = MyClass()
        instance2 = MyClass()

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1, instance2)

    def test_singleton_with_arguments(self):
        class MyClassWithArguments(metaclass=Singleton):
            def __init__(self, value):
                self.value = value

        instance1 = MyClassWithArguments(value=42)
        instance2 = MyClassWithArguments(value=99)

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.value, instance2.value)

    def test_singleton_with_kwargs(self):
        class MyClassWithKwargs(metaclass=Singleton):
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        instance1 = MyClassWithKwargs(name="Alice")
        instance2 = MyClassWithKwargs(name="Bob")

        self.assertIs(instance1, instance2)
        self.assertEqual(instance1.kwargs, instance2.kwargs)
