import turtle

wn = turtle.Screen()

wn.bgcolor('#002a4a')

t = turtle.Turtle()

t.fillcolor('#FFCB05')

t.speed(10)

t.up()
t.goto(-160,-160)
t.down()

t.begin_fill()

t.forward(125)
t.left(90)
t.forward(70)
t.left(90)
t.forward(30)
t.right(90)
t.forward(65)
t.right(90+90-38)
t.forward(114)
t.left(142-90+52) ##
t.forward(114)
t.right(180-38)
t.forward(65)
t.right(90)
t.forward(30)
t.left(90)
t.forward(70)
t.left(90)
t.forward(125) ##
t.left(90)
t.forward(65)
t.left(90)
t.forward(25)
t.right(90)
t.forward(105)
t.right(90)
t.forward(25)
t.left(90)
t.forward(65)
t.left(90)
t.forward(100) ##
t.left(52)
t.forward(115)
t.right(142-90+52)
t.forward(115)
t.left(52)
t.forward(89-0.231322951) ##*-
t.left(90)
t.forward(65)
t.left(90)
t.forward(25)
t.right(90)
t.forward(100)
t.right(90)
t.forward(25)
t.left(90)
t.forward(70)
t.goto(-160,-160)

t.end_fill()
