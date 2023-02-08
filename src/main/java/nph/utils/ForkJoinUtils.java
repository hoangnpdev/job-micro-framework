package nph.utils;

import org.apache.log4j.Logger;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Deprecated
public class ForkJoinUtils {

	private static Logger log = Logger.getLogger(ForkJoinUtils.class);

	public static void splitDatasetByDate(String localPath, String outDir, boolean isAppend,
										  int datePos, String dateFormat,
										  int cellDatePos, String cellDateFormat, String newCellDateFormat,
										  int... removePos) throws IOException {

		BufferedReader reader = new BufferedReader(new FileReader(localPath));
		AtomicReference<String> outputFilename = new AtomicReference<>();
		outputFilename.set("20210101");
		new File(outDir + outputFilename.get() + "/").mkdirs();
		AtomicReference<BufferedWriter> writer = new AtomicReference<>(
				new BufferedWriter(new FileWriter(outDir + outputFilename.get() + "/" + outputFilename.get() + ".csv", isAppend))
		);

		String finalOutDir = outDir;
		boolean finalIsAppend = isAppend;
		reader.lines().forEach(line -> {
			try {
				List<String> cells = new LinkedList<>(Arrays.asList(line.split(",")));
				String dateStr = cells.get(datePos);
				// 2021-01-01 00:00:01
				Date date = DateUtils.str2Date(dateStr, dateFormat);
				String newDate = DateUtils.date2Str(date, "yyyyMMdd");

				// file selection
				if (!outputFilename.get().equals(newDate)) {
					log.info(outputFilename + " " + newDate + " " + dateStr + " " + line);
					outputFilename.set(newDate);
					writer.get().close();
					new File(finalOutDir + newDate + "/").mkdirs();
					writer.set(new BufferedWriter(new FileWriter(finalOutDir + newDate + "/" + newDate + ".csv", finalIsAppend)));
				}

				// reformat date
				Date newCellDate = DateUtils.str2Date(cells.get(cellDatePos), cellDateFormat);
				String newCellDateStr = DateUtils.date2Str(newCellDate, newCellDateFormat);
				cells.set(cellDatePos, newCellDateStr);






				// remove should be last logic
				for (int re: removePos)
					cells.remove(re);
				// save
				writer.get().write(String.join(",", cells));
				writer.get().newLine();
			} catch (IOException | ParseException e) {
				e.printStackTrace();
			}
		});
		writer.get().close();
	}
}
