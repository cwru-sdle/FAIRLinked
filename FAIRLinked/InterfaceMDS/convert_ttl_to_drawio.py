from cemento.rdf.read_turtle import ReadTurtle
from cemento.tree import Tree
from cemento.draw_io.write_diagram import WriteDiagram

def convert_ttl_to_cemento(ttl_input_path, drawio_output_path):
    ex = ReadTurtle(ttl_input_path)
    tree = Tree(graph=ex.get_graph(), do_gen_ids=True, invert_tree=True)
    diagram = WriteDiagram(drawio_output_path)
    tree.draw_tree(write_diagram=diagram)
    diagram.draw()