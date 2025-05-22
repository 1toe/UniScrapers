pacientes = []
medicamentos = []

def validar_rut(rut):
    return False if rut.strip() == "" else True

def buscar_paciente(rut):
    for i in range(len(pacientes)):
        if pacientes[i][3] == rut:
            return i
    return -1

def ingresar_paciente():
    print("\n ------------\n")
    print("Ingreso de Paciente")
    num_ficha = f"F{len(pacientes) + 1}"
    print(f"Número de Ficha: {num_ficha}")
    nombre = input("Nombre del paciente: ")
    apellido = input("Apellido del paciente: ")
    rut = ""
    while not validar_rut(rut):
        rut = input("RUT del paciente (obligatorio): ")
        if not validar_rut(rut):
            print("Error: El RUT no puede estar vacío.")
    if buscar_paciente(rut) != -1:
        print("\nEl paciente ya existe en el sistema.")
        input("Presione Enter para continuar...")
        return
    sexo = input("Sexo (M/F): ")
    edad = input("Edad: ")
    estado_civil = input("Estado Civil: ")
    domicilio = input("Domicilio: ")
    telefono = input("Teléfono: ")
    grupo_sanguineo = input("Grupo Sanguíneo: ")
    acompanado = input("¿Asiste acompañado? (S/N): ").upper()
    datos_acompanante = []
    if acompanado == "S":
        nombre_acomp = input("Nombre del acompañante: ")
        apellido_acomp = input("Apellido del acompañante: ")
        rut_acomp = input("RUT del acompañante: ")
        parentesco = input("Grado de parentesco: ")
        telefono_acomp = input("Teléfono del acompañante: ")
        datos_acompanante = [nombre_acomp, apellido_acomp, rut_acomp, parentesco, telefono_acomp]
    motivo_consulta = input("Motivo de consulta médica: ")
    descripcion_paciente = input("Descripción del paciente: ")
    nombre_medico = input("Nombre del médico: ")
    especialidad = input("Especialidad del médico: ")
    sintomas = input("Síntomas detectados: ")
    diagnostico = input("Diagnóstico: ")
    reposo = input("¿Requiere reposo? (S/N): ").upper()
    dias_reposo = "0"
    if reposo == "S":
        dias_reposo = input("Cantidad de días de reposo: ")
    paciente = [num_ficha, nombre, apellido, rut, sexo, edad, estado_civil, domicilio, telefono, 
                grupo_sanguineo, acompanado, datos_acompanante, motivo_consulta, descripcion_paciente,
                nombre_medico, especialidad, sintomas, diagnostico, reposo, dias_reposo]
    pacientes.append(paciente)
    asigna_medicamento = input("¿El médico asigna medicamento? (S/N): ").upper()
    if asigna_medicamento == "S":
        continuar = "S"
        while continuar == "S":
            nombre_med = input("Nombre del medicamento: ")
            dosis = input("Dosis: ")
            dias = input("Cantidad de días: ")
            medicamento = [rut, nombre_med, dosis, dias]
            medicamentos.append(medicamento)
            continuar = input("¿Desea agregar otro medicamento? (S/N): ").upper()
    print("\nFicha ingresada correctamente.")
    input("Presione Enter para continuar...")

def mostrar_paciente():
    print("\n ------------\n")
    print("Buscar Paciente")
    rut = input("Ingrese el RUT del paciente: ")
    if not validar_rut(rut):
        print("Error: El RUT no puede estar vacío.")
        input("Presione Enter para continuar...")
        return
    indice = buscar_paciente(rut)
    if indice == -1:
        print("\nEl paciente no existe en el sistema.")
        input("Presione Enter para continuar...")
        return
    paciente = pacientes[indice]
    print("\n ------------\n")
    print("Datos del Paciente")
    print(f"Número de Ficha: {paciente[0]}")
    print(f"Nombre: {paciente[1]} {paciente[2]}")
    print(f"RUT: {paciente[3]}")
    print(f"Sexo: {paciente[4]}")
    print(f"Edad: {paciente[5]}")
    print(f"Estado Civil: {paciente[6]}")
    print(f"Domicilio: {paciente[7]}")
    print(f"Teléfono: {paciente[8]}")
    print(f"Grupo Sanguíneo: {paciente[9]}")
    if paciente[10] == "S":
        print("\n ------------\n")
        print("Datos del Acompañante")
        acomp = paciente[11]
        print(f"Nombre: {acomp[0]} {acomp[1]}")
        print(f"RUT: {acomp[2]}")
        print(f"Parentesco: {acomp[3]}")
        print(f"Teléfono: {acomp[4]}")
    print("\n ------------\n")
    print("Datos Médicos")
    print(f"Motivo de Consulta: {paciente[12]}")
    print(f"Descripción del Paciente: {paciente[13]}")
    print(f"Médico: {paciente[14]}")
    print(f"Especialidad: {paciente[15]}")
    print(f"Síntomas: {paciente[16]}")
    print(f"Diagnóstico: {paciente[17]}")
    print(f"Reposo: {'Sí, por ' + paciente[19] + ' días' if paciente[18] == 'S' else 'No'}")
    input("\nPresione Enter para continuar...")

def mostrar_medicamentos():
    print("\n ------------\n")
    print("Mostrar Medicamentos de Paciente")
    rut = input("Ingrese el RUT del paciente: ")
    if not validar_rut(rut):
        print("Error: El RUT no puede estar vacío.")
        input("Presione Enter para continuar...")
        return
    indice = buscar_paciente(rut)
    if indice == -1:
        print("\nEl paciente no existe en el sistema.")
        input("Presione Enter para continuar...")
        return
    paciente = pacientes[indice]
    meds_paciente = [med for med in medicamentos if med[0] == rut]
    if len(meds_paciente) == 0:
        print(f"\nEl paciente {paciente[1]} {paciente[2]} no tiene medicamentos recetados.")
        input("Presione Enter para continuar...")
        return
    print(f"\nMedicamentos recetados a {paciente[1]} {paciente[2]}:\n")
    for i, med in enumerate(meds_paciente):
        print(f"Medicamento {i+1}:")
        print(f"Nombre: {med[1]}")
        print(f"Dosis: {med[2]}")
        print(f"Duración: {med[3]} días")
        print()
    input("Presione Enter para continuar...")

def eliminar_paciente():
    print("\n ------------\n")
    print("Eliminar Paciente")
    rut = input("Ingrese el RUT del paciente: ")
    if not validar_rut(rut):
        print("Error: El RUT no puede estar vacío.")
        input("Presione Enter para continuar...")
        return
    indice = buscar_paciente(rut)
    if indice == -1:
        print("\nEl paciente no existe en el sistema.")
        input("Presione Enter para continuar...")
        return
    paciente = pacientes[indice]
    print(f"\n¿Está seguro que desea eliminar la ficha de {paciente[1]} {paciente[2]}?")
    if input("Ingrese S para confirmar, cualquier otra tecla para cancelar: ").upper() != "S":
        print("\nOperación cancelada.")
        input("Presione Enter para continuar...")
        return
    pacientes.pop(indice)
    i = 0
    while i < len(medicamentos):
        if medicamentos[i][0] == rut:
            medicamentos.pop(i)
        else:
            i += 1
    print("\nFicha eliminada correctamente.")
    input("Presione Enter para continuar...")

def listar_pacientes():
    print("\n ------------ n")
    print("\n Lista de Pacientes")
    if len(pacientes) == 0:
        print("No hay pacientes registrados en el sistema.")
        input("Presione Enter para continuar...")
        return
    print(f"Total de pacientes: {len(pacientes)}\n")
    for i, paciente in enumerate(pacientes):
        print(f"Paciente {i+1}:")
        print(f"Ficha: {paciente[0]}")
        print(f"Nombre: {paciente[1]} {paciente[2]}")
        print(f"RUT: {paciente[3]}")
        print(f"Diagnóstico: {paciente[17]}")
        print()
    input("Presione Enter para continuar...")

def validar_opcion(opcion):
    try:
        opcion = int(opcion)
        return True if 1 <= opcion <= 6 else False
    except:
        return False

def mostrar_menu():
    print("\n ------------\n")
    print("1. Ingresar ficha de paciente")
    print("2. Buscar paciente por RUT")
    print("3. Mostrar medicamentos de paciente por RUT")
    print("4. Eliminar ficha de paciente")
    print("5. Listar todos los pacientes")
    print("6. Salir")
    print("\n ------------ ")

def main():
    while True:
        mostrar_menu()
        opcion = input("Ingrese una opción (1-6): ")
        if not validar_opcion(opcion):
            print("\nError: Opción inválida. Debe ser un número entre 1 y 6.")
            input("Presione Enter para continuar...")
            continue
        opcion = int(opcion)
        if opcion == 1: ingresar_paciente()
        elif opcion == 2: mostrar_paciente()
        elif opcion == 3: mostrar_medicamentos()
        elif opcion == 4: eliminar_paciente()
        elif opcion == 5: listar_pacientes()
        elif opcion == 6:
            print("¡Gracias por utilizar el Sistema de Gestión de Fichas Clínicas de la Clínica Empire!")
            break
main()
