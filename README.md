# Ashley Furniture Service

Get all products
```bash 
    curl -X GET http://localhost:8080/products    
```

Response example:
```json 
[
  {
    "nombre": "Twin Memory Foam Mattress",
    "clave": "100-10",
    "categoria": "ZZ",
    "modelo": " 100",
    "costo": 111.1,
    "costo2": 113.32,
    "proveedor": "Ashley Furniture",
    "cantidadSillas": 0,
    "cantidadPorPaquete": 1,
    "descontinuado": "Current",
    "alto": 114.3,
    "largo": 812.8,
    "ancho": 1828.8,
    "peso": 7.26
  },
  {
    ...
  },
]

```
