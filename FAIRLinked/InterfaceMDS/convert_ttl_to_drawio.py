from cemento.rdf.read_turtle import ReadTurtle
from cemento.tree import Tree
from cemento.draw_io.write_diagram import WriteDiagram

def convert_ttl_to_cemento(ttl_input_path, drawio_output_path):
    """
    This function uses the CEMENTO package to generate a draw.io diagram from a turtle file, allowing for better visualization
    of ontologies. 

    Args:
        ttl_input_path: Path to turtle file
        drawio_output_path: Path to store draw.io diagram

    Returns:
        None
    """

    ex = ReadTurtle(ttl_input_path)
    tree = Tree(graph=ex.get_graph(), do_gen_ids=True, invert_tree=True)
    diagram = WriteDiagram(drawio_output_path)
    tree.draw_tree(write_diagram=diagram)
    diagram.draw()