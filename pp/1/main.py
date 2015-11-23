import sys, re
from handlers import *
from util import *
from rules import *

class Parser:
    def __init__(self,handler):
        self.handler = handler
        self.rules = []
        self.filters = []

    def addRule(self, rule):
        self.rules.append(rule)

    def addFilter(self,pattern,name):
        def filter(block, handler):
            return re.sub(pattern, handler.sub(name),block)
        self.filters.append(filter)

    def parse(self, file):
        self.handler.start('document')
        for block in blocks(file):
            for filter in self.filters:
                block = filter(block, self.handler)
            for rule in self.rules:
                if rule.condition(block):
                    last = rule.action(block, self.handler)
                    if last:break
        self.handler.end('document')

class BasicTextParser(Parser):
    def __init__(self,handler):
        Parser.__init__(self,handler)
        self.addRule(ListRule())
        self.addRule(ListItemRule())
        self.addRule(TitleRule())
        self.addRule(HeadingRule())
        self.addRule(ParagraphRule())

        self.addFilter(r'\*(.+?)\*', 'emphasis')
        self.addFilter(r'(http://[\.a-z0-9A-Z/]+)', 'url')
        self.addFilter(r'([\.a-zA-Z]+@[\.a-zA-Z]+[a-zA-Z]+)','mail')

handler = HTMLRenderer()
parser = BasicTextParser(handler)
html='''  PYTHON
        想用python重写一遍，不料我身边已经有同学捷足先登了，
提前实现了，这即使我做完之后都不好意思在他面前装逼了，
不知道各位还有没有发现别的不错的python项目，
最好是综合性的，不要是网站，
谢了，
题主是一个计算机相关专业大三学生，有一定编程基础。
'''
parser.parse(html)
