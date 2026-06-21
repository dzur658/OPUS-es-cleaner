from pysbd.utils import PySBDFactory
import spacy
from spacy.language import Language

@Language.factory("pysbd_es")
def pysbd_component(nlp, name):
    return PySBDFactory(nlp, language="es")

nlp = spacy.blank('es')

nlp.add_pipe('pysbd_es')

raw_text = """\
Ella no ha hecho nada.
¡Ahora déjele solo!
¡No, no!
¡No mientas por mí!
Que tenga una meditación agradable.
Los doctores estan seguros que su suegro estaba muerto antes de que cayera al agua.
Su mizzen se movio así. y e sel fue por la borda.
Lo hice lo mas rapido que pude.
Nadie me vio, estoy seguro.
¿Cómo se llama usted?
Johnny.
Papá.
00:
00:
23, 731--> 00:
00:
24, 815 Espere un minuto, CoIumbo.
NETKonet trae para ti...
"Twenty"
[NEW Presenta]
[Una producción de A M Tree Pictures]
[Sidus HQ]
[Productor ejecutivo:
Kim Woo Taek]
Somos los típicos amigos de la secundaria que juraron que su amistad duraría para siempre.
Aunque las circunstancias del inicio de nuestra amistad no fueron especiales en cierto modo fueron especiales para nosotros.
Ejem... ejem.
Ejem...
[Encuéntrate conmigo en el patio de atrás.]
- ¿Le acariciaste el pecho a Fo Min?
- No.
No se lo acaricié, me quede allí parado tocándolo.
- Debes estar demente.
- ¿Eso es una pregunta?
¿Crees que fue una pregunta, idiota?
Ey, que ya no somos niños.
"""

doc = nlp(raw_text)
print(list(doc.sents))