rm -rfv gate
rm -rfv transport
hot --out gate --makefile --stub --client gate.hot
hot --out transport --makefile --stub --client transport.hot
