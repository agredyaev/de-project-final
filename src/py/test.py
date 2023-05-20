class A:
    def method_one():
        print('method_one')

    def method_two():
        print('method_two')




def caller(method: str):
    getattr(A, method)()



caller('method_two')