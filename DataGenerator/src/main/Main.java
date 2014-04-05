package main;

import java.io.IOException;
import util.ItemGenerator;

public class Main {

	public static void main(String[] args) throws IOException {
		
		
		// Comando da usare per generare il dataset
		
		// prima bisogna configurare il generatore di nomi e interessi
		/* al costruttore vanno passati due file: il primo contiene le sillabe
		 * con cui generare i nomi e il secondo contiene in chiaro la lista di
		 * interessi da cui pescare
		 * 
		 * tali file sono in due cartelle: syllable e interest
		 * il file hobby pu� tranquillamente essere editato aggiungendo nuovi
		 * iteressi (uno per riga)
		 */
		ItemGenerator IG = new ItemGenerator("syllable/elven","interest/hobby");
		
		/* quindi bisogna richiamare la funzione generate in cui bisogna passare:
		 * - il nome del file in cui generare il dataset
		 * - il numero di righe del file (nell'esempio 10)
		 * - il numero massimo di sillabe contenute in un nome (nell'esempio 4)
		 * - il numero massimo di interessi per nome (nell'esempio 5)
		 */
		IG.generate("data/esempio.txt", 1000, 5, 5);

	}

}
