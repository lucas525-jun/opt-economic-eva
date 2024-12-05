package mx.com.innovating.cloud.orchestrator.services;


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class ImpuestosService {

    @Inject
    DataBaseConnectorRepository dataBaseConnectorRepository;




    public byte[] generateExcelImpuestos(Integer idOportunidad, Integer version) {
        // Obtener datos desde el repositorio
        Map<String, List<Map<String, Object>>> dataMap = Map.of(
                "Derecho Extraccion de Hidrocarburos", dataBaseConnectorRepository.getDEH(idOportunidad, version),
                "Derecho de Utilidad Compartida", dataBaseConnectorRepository.getDUC(idOportunidad, version),
                "Impuesto Uso Superficial", dataBaseConnectorRepository.getIUS(idOportunidad, version),
                "Cuota Contractual para la Fase Exploratoria", dataBaseConnectorRepository.getCCO(idOportunidad, version),
                "Impuesto Actividad de Exploracion y Extraccion", dataBaseConnectorRepository.getIEO(idOportunidad, version),
                "Impuesto Sobre la Renta", dataBaseConnectorRepository.getISR(idOportunidad, version)
        );

        try (Workbook workbook = new XSSFWorkbook()) {
            Map<String, Row> totalRows = new LinkedHashMap<>();

            // Crear hojas con datos transformados
            for (Map.Entry<String, List<Map<String, Object>>> entry : dataMap.entrySet()) {
                String sheetName = entry.getKey();
                List<Map<String, Object>> rawData = entry.getValue();
                Sheet sheet = addDataToSheet(workbook, sheetName, rawData);

                // Guardar la última fila (total) de cada hoja
                totalRows.put(sheetName, sheet.getRow(sheet.getLastRowNum()));
            }

            // Crear la hoja de totales
            addSummarySheet(workbook, totalRows);

            // Escribir el contenido del Workbook en un array de bytes
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                workbook.write(outputStream);
                return outputStream.toByteArray();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error al generar el archivo Excel", e);
        }
    }

    private void addSummarySheet(Workbook workbook, Map<String, Row> totalRows) {
        Sheet summarySheet = workbook.createSheet("Totales");
        CellStyle headerStyle = createHeaderStyle(workbook);
        CellStyle numericStyle = createNumericStyle(workbook);

        // Crear encabezados
        Row headerRow = summarySheet.createRow(0);
        headerRow.createCell(0).setCellValue("Tipo");
        headerRow.getCell(0).setCellStyle(headerStyle);

        // Obtener todos los años posibles de las hojas
        Set<String> allYears = totalRows.values().stream()
                .flatMap(row -> {
                    List<String> years = new ArrayList<>();
                    for (int i = 1; i < row.getLastCellNum(); i++) {
                        years.add(row.getSheet().getRow(0).getCell(i).getStringCellValue());
                    }
                    return years.stream();
                })
                .collect(Collectors.toSet());

        List<String> sortedYears = new ArrayList<>(allYears);
        Collections.sort(sortedYears);

        // Encabezados de años
        for (int i = 0; i < sortedYears.size(); i++) {
            Cell cell = headerRow.createCell(i + 1);
            cell.setCellValue(sortedYears.get(i));
            cell.setCellStyle(headerStyle);
        }

        // Agregar filas de totales por cada hoja
        int rowIndex = 1;
        for (Map.Entry<String, Row> entry : totalRows.entrySet()) {
            String sheetName = entry.getKey();
            Row totalRow = entry.getValue();
            Row row = summarySheet.createRow(rowIndex++);
            row.createCell(0).setCellValue(sheetName);

            for (int i = 1; i <= sortedYears.size(); i++) {
                double total = 0.0;
                for (int j = 1; j < totalRow.getLastCellNum(); j++) {
                    String year = totalRow.getSheet().getRow(0).getCell(j).getStringCellValue();
                    if (sortedYears.get(i - 1).equals(year)) {
                        total = totalRow.getCell(j).getNumericCellValue();
                        break;
                    }
                }
                row.createCell(i).setCellValue(total);
                row.getCell(i).setCellStyle(numericStyle);
            }
        }

        // Calcular totales por año
        Row grandTotalRow = summarySheet.createRow(rowIndex);
        grandTotalRow.createCell(0).setCellValue("Total Impuestos");
        grandTotalRow.getCell(0).setCellStyle(headerStyle);
        for (int i = 1; i <= sortedYears.size(); i++) {
            double grandTotal = 0.0;
            for (int j = 1; j < rowIndex; j++) {
                Row dataRow = summarySheet.getRow(j);
                Cell cell = dataRow.getCell(i);
                if (cell != null) {
                    grandTotal += cell.getNumericCellValue();
                }
            }
            Cell cell = grandTotalRow.createCell(i);
            cell.setCellValue(grandTotal);
            cell.setCellStyle(numericStyle);
        }

        // Ajustar ancho de columnas
        for (int i = 0; i <= sortedYears.size(); i++) {
            summarySheet.autoSizeColumn(i);
        }
    }
    private Sheet addDataToSheet(Workbook workbook, String sheetName, List<Map<String, Object>> rawData) {
        Sheet sheet = workbook.createSheet(sheetName);
        Map<String, Map<String, Double>> transformedData = transformData(sheetName, rawData);

        // Obtener años únicos como encabezados
        List<String> years = transformedData.values().stream()
                .flatMap(map -> map.keySet().stream())
                .distinct()
                .sorted()
                .toList();

        // Crear estilos y filas
        CellStyle headerStyle = createHeaderStyle(workbook);
        CellStyle numericStyle = createNumericStyle(workbook);
        CellStyle totalStyle = createTotalStyle(workbook);

        createHeaderRow(sheet, years, headerStyle);
        createDataRows(sheet, transformedData, years, numericStyle);
        if(sheetName.equals("Derecho Extraccion de Hidrocarburos")){
            addTotalRow(sheet, years.size(), totalStyle);
        }



        // Ajustar ancho de columnas
        for (int i = 0; i <= years.size(); i++) {
            sheet.autoSizeColumn(i);
        }

        return sheet;
    }

    private Map<String, Map<String, Double>> transformData(String sheetName, List<Map<String, Object>> rawData) {
        switch (sheetName) {
            case "Derecho Extraccion de Hidrocarburos":
                return transformDataDEH(rawData);
            case "Derecho de Utilidad Compartida":
                return transformDataDUC(rawData);
            case "Impuesto Sobre la Renta":
                return transformDataISR(rawData);
            default:
                return transformDataGeneral(rawData, sheetName);
        }
    }

    private void createHeaderRow(Sheet sheet, List<String> years, CellStyle headerStyle) {
        Row headerRow = sheet.createRow(0);
        headerRow.createCell(0).setCellValue("Tipo");
        headerRow.getCell(0).setCellStyle(headerStyle);

        for (int i = 0; i < years.size(); i++) {
            Cell cell = headerRow.createCell(i + 1);
            cell.setCellValue(years.get(i));
            cell.setCellStyle(headerStyle);
        }
    }

    private void createDataRows(Sheet sheet, Map<String, Map<String, Double>> data, List<String> years, CellStyle numericStyle) {
        int rowIndex = 1;
        for (String type : data.keySet()) {
            Row row = sheet.createRow(rowIndex++);
            Cell typeCell = row.createCell(0);
            typeCell.setCellValue(type);
            typeCell.setCellStyle(numericStyle);

            Map<String, Double> valuesByYear = data.get(type);
            for (int i = 0; i < years.size(); i++) {
                String year = years.get(i);
                Double value = valuesByYear.getOrDefault(year, 0.0);
                Cell cell = row.createCell(i + 1);
                cell.setCellValue(value);
                cell.setCellStyle(numericStyle);
            }
        }
    }

    private void addTotalRow(Sheet sheet, int numberOfColumns, CellStyle totalStyle) {
        int rowIndex = sheet.getLastRowNum() + 1;
        Row totalRow = sheet.createRow(rowIndex);
        Cell totalLabelCell = totalRow.createCell(0);
        totalLabelCell.setCellValue(sheet.getSheetName());
        totalLabelCell.setCellStyle(totalStyle);

        for (int i = 1; i <= numberOfColumns; i++) {
            double sum = 0.0;
            for (int j = 1; j < rowIndex; j++) {
                Row dataRow = sheet.getRow(j);
                Cell cell = dataRow.getCell(i);
                if (cell != null) {
                    sum += cell.getNumericCellValue();
                }
            }
            Cell totalCell = totalRow.createCell(i);
            totalCell.setCellValue(sum);
            totalCell.setCellStyle(totalStyle);
        }
    }

    private CellStyle createHeaderStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.DARK_RED.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        Font font = workbook.createFont();
        font.setBold(true);
        font.setColor(IndexedColors.WHITE.getIndex());
        style.setFont(font);
        return style;
    }

    private CellStyle createNumericStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.RIGHT);
        return style;
    }

    private CellStyle createTotalStyle(Workbook workbook) {
        CellStyle style = createHeaderStyle(workbook);
        style.setAlignment(HorizontalAlignment.RIGHT);
        return style;
    }

    // Transformaciones específicas (mantengo como métodos independientes)
    private Map<String, Map<String, Double>> transformDataGeneral(List<Map<String, Object>> queryResult, String tipo) {
        // Transformación genérica
        Map<String, Map<String, Double>> transformedData = new LinkedHashMap<>();
        for (Map<String, Object> row : queryResult) {
            String year = row.get("anio").toString();
            Double value = row.get("imp") != null ? Double.parseDouble(row.get("imp").toString()) : 0.0;
            transformedData.computeIfAbsent(tipo, k -> new LinkedHashMap<>()).put(year, value);
        }
        return transformedData;
    }
    private Map<String, Map<String, Double>> transformDataISR(List<Map<String, Object>> queryResult){
        Map<String, Map<String, Double>> transformedData = new LinkedHashMap<>();
        List<String> tipoImp = List.of(
                "Inversion a depreciar al 10% (mmpesos)",
                "Inversion a depreciar al 25% (mmpesos)",
                "Total depreciacion",
                "Costo Real",
                "Impuesto Sobre la Renta"
        );

        for (Map<String, Object> row : queryResult) {
            String anio = row.get("anio").toString(); // Año de la fila
            // Valores para cada tipo
            Double vdeduccion10 = row.get("vdeduccion10") != null ? Double.parseDouble(row.get("vdeduccion10").toString()) : 0.0;
            Double vdeduccion25 = row.get("vdeduccion25") != null ? Double.parseDouble(row.get("vdeduccion25").toString()) : 0.0;
            Double impDepreciacion = row.get("impdepreciacion") != null ? Double.parseDouble(row.get("impdepreciacion").toString()) : 0.0;
            Double costoReal = row.get("vcosto") != null ? Double.parseDouble(row.get("vcosto").toString()) : 0.0;
            Double isr = row.get("visr") != null ? Double.parseDouble(row.get("visr").toString()) : 0.0;


            // Asignar los valores a cada tipo en el mapa
            transformedData
                    .computeIfAbsent(tipoImp.get(0), k -> new LinkedHashMap<>())
                    .put(anio, vdeduccion10);

            transformedData
                    .computeIfAbsent(tipoImp.get(1), k -> new LinkedHashMap<>())
                    .put(anio, vdeduccion25);

            transformedData
                    .computeIfAbsent(tipoImp.get(2), k -> new LinkedHashMap<>())
                    .put(anio, impDepreciacion);

            transformedData
                    .computeIfAbsent(tipoImp.get(3), k -> new LinkedHashMap<>())
                    .put(anio, costoReal);
            transformedData
                    .computeIfAbsent(tipoImp.get(4), k -> new LinkedHashMap<>())
                    .put(anio, isr);

        }
        return transformedData;
    }


    private Map<String, Map<String, Double>> transformDataDUC(List<Map<String, Object>> queryResult) {
        Map<String, Map<String, Double>> transformedData = new LinkedHashMap<>();

        // Lista de tipos de impuestos
        List<String> tipoImp = List.of(
                "Acumulador DUC 1",
                "Acumulador DUC 2 (Costos recuperados)",
                "Acumulador DUC 3",
                "Derecho de Utilidad Compartida"
        );

        for (Map<String, Object> row : queryResult) {
            String anio = row.get("anio").toString(); // Año de la fila
            // Valores para cada tipo
            Double duc1 = row.get("vduc1") != null ? Double.parseDouble(row.get("vduc1").toString()) : 0.0;
            Double duc2 = row.get("vduc2") != null ? Double.parseDouble(row.get("vduc2").toString()) : 0.0;
            Double duc3 = row.get("vduc3") != null ? Double.parseDouble(row.get("vduc3").toString()) : 0.0;
            Double ducDerecho = row.get("vderecho") != null ? Double.parseDouble(row.get("vderecho").toString()) : 0.0;

            // Asignar los valores a cada tipo en el mapa
            transformedData
                    .computeIfAbsent(tipoImp.get(0), k -> new LinkedHashMap<>())
                    .put(anio, duc1);

            transformedData
                    .computeIfAbsent(tipoImp.get(1), k -> new LinkedHashMap<>())
                    .put(anio, duc2);

            transformedData
                    .computeIfAbsent(tipoImp.get(2), k -> new LinkedHashMap<>())
                    .put(anio, duc3);
            transformedData
                    .computeIfAbsent(tipoImp.get(3), k -> new LinkedHashMap<>())
                    .put(anio, ducDerecho);

        }

        return transformedData;
    }





    private Map<String, Map<String, Double>> transformDataDEH(List<Map<String, Object>> queryResult) {
        Map<String, Map<String, Double>> transformedData = new LinkedHashMap<>();

        for (Map<String, Object> row : queryResult) {
            String anio = row.get("anio").toString();
            String tipo = row.get("tipo").toString();
            Double duh = row.get("duh") != null ? Double.parseDouble(row.get("duh").toString()) : 0.0;

            transformedData
                    .computeIfAbsent(tipo, k -> new LinkedHashMap<>())
                    .put(anio, duh);
        }

        return transformedData;
    }

}


 /*


        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Datos");

            // Transformar los datos
            Map<String, Map<String, Double>> data = transformData(infoDEH);

            // Obtener años únicos como encabezados
            List<String> years = new ArrayList<>(data.values().stream()
                    .flatMap(map -> map.keySet().stream())
                    .collect(Collectors.toSet()));

            Collections.sort(years); // Ordenar años

            // Crear estilos
            CellStyle headerStyle = workbook.createCellStyle();
            headerStyle.setFillForegroundColor(IndexedColors.DARK_RED.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            headerStyle.setAlignment(HorizontalAlignment.CENTER);
            headerStyle.setVerticalAlignment(VerticalAlignment.CENTER);
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerFont.setColor(IndexedColors.WHITE.getIndex());
            headerStyle.setFont(headerFont);

            CellStyle numericStyle = workbook.createCellStyle();
            numericStyle.setAlignment(HorizontalAlignment.RIGHT);

            CellStyle totalStyle = workbook.createCellStyle();
            totalStyle.setFillForegroundColor(IndexedColors.DARK_RED.getIndex());
            totalStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            totalStyle.setAlignment(HorizontalAlignment.RIGHT);
            totalStyle.setFont(headerFont);

            // Crear encabezados
            Row headerRow = sheet.createRow(0);
            headerRow.createCell(0).setCellValue("Tipo");
            headerRow.getCell(0).setCellStyle(headerStyle);

            for (int i = 0; i < years.size(); i++) {
                Cell cell = headerRow.createCell(i + 1);
                cell.setCellValue(years.get(i));
            }

            // Crear filas de datos
            int rowIndex = 1;
            for (String tipo : data.keySet()) {
                Row row = sheet.createRow(rowIndex++);
                Cell typeCell = row.createCell(0);
                typeCell.setCellValue(tipo);
                typeCell.setCellStyle(headerStyle);


                Map<String, Double> valuesByYear = data.get(tipo);
                for (int i = 0; i < years.size(); i++) {
                    String year = years.get(i);
                    Double value = valuesByYear.getOrDefault(year, 0.0); // 0.0 si no hay valor para ese año
                    Cell cell = row.createCell(i + 1);
                    cell.setCellValue(value);
                    cell.setCellStyle(numericStyle);
                }
            }

            // Calcular totales por cada columna (año)
            Row totalRow = sheet.createRow(rowIndex); // Fila para los totales
            Cell totalLabelCell = totalRow.createCell(0);
            totalLabelCell.setCellValue("Total");
            totalLabelCell.setCellStyle(totalStyle);

            for (int i = 0; i < years.size(); i++) {
                double sum = 0.0;
                for (int j = 1; j < rowIndex; j++) { // Recorremos las filas de datos (excluyendo el encabezado)
                    Row dataRow = sheet.getRow(j);
                    Cell cell = dataRow.getCell(i + 1);
                    if (cell != null) {
                        sum += cell.getNumericCellValue();
                    }
                }
                Cell totalCell = totalRow.createCell(i + 1);
                totalCell.setCellValue(sum);
                totalCell.setCellStyle(totalStyle);
            }

            // Ajustar el ancho de las columnas
            for (int i = 0; i <= years.size(); i++) {
                sheet.autoSizeColumn(i);
            }

            // Escribir el contenido en un flujo de salida
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            return outputStream.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Error al generar el archivo Excel", e);
        }

    }
    */