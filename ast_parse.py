import ast

code = "x = 1 + 2"
tree = ast.parse(code)
print(ast.dump(tree, indent=4))
