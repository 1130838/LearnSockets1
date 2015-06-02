import java.io.*; 
import java.net.*; 
import java.util.concurrent.*;

// IntelliJ: Edit configurations : colocar em Program arguments o IP local ( ver centro de Rede e Partilha - ligação área locaçl - detalhes )
// correr server 1º. depois client

class tcp_chat_srv
{    
public static int MAXCLI=100;
public static Socket[] cliSock = new Socket[MAXCLI];
public static DataOutputStream[] sOut = new DataOutputStream[MAXCLI];
public static Boolean[] inUse= new Boolean[MAXCLI];
public static Semaphore changeLock = new Semaphore(1);

static ServerSocket sock;

public static void main(String args[]) throws Exception    
{       
int i;

try { sock = new ServerSocket(9999); }
catch(IOException ex) {
	System.out.println("O porto local esta ocupado.");
	System.exit(1); }

for(i=0; i<MAXCLI; i++) inUse[i]=false;
while(true)
	{
	changeLock.acquire();
	for(i=0; i<MAXCLI; i++) if(!inUse[i]) break; // procurar um socket livre
	changeLock.release();
	cliSock[i]=sock.accept(); // esperar por lig. de novo cliente
	changeLock.acquire();
	inUse[i]=true;
	sOut[i]=new DataOutputStream(cliSock[i].getOutputStream());
	changeLock.release();
	new Thread(new tcp_chat_srv_conn(i)).start();
	}
} 
}



class tcp_chat_srv_conn implements Runnable 
{    
int myNum;
private DataInputStream sIn;

public tcp_chat_srv_conn(int cli_n) { myNum=cli_n;}

public void run()
{
int i, nChars;
byte[] data = new byte[300];

try {
sIn = new DataInputStream(tcp_chat_srv.cliSock[myNum].getInputStream());
while(true)
   {
   nChars=sIn.read();
   if(nChars==0) break; // linha vazia
   sIn.read(data,0,nChars);
   tcp_chat_srv.changeLock.acquire();
   for(i=0; i<tcp_chat_srv.MAXCLI; i++) // retransmitir a linha
	if(tcp_chat_srv.inUse[i])
		{
		tcp_chat_srv.sOut[i].write(nChars);
		tcp_chat_srv.sOut[i].write(data,0,nChars);
		}
   tcp_chat_srv.changeLock.release();
   }
   // o cliente quer sair
   tcp_chat_srv.changeLock.acquire();
   tcp_chat_srv.sOut[myNum].write(nChars);
   tcp_chat_srv.inUse[myNum]=false;
   tcp_chat_srv.cliSock[myNum].close();
   tcp_chat_srv.changeLock.release();
} catch(IOException ex) { System.out.println("IOException"); }
  catch(InterruptedException ex) { System.out.println("Interrupted"); }
}
}
