package com.piranha.storageapi.service;




import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.piranha.storageapi.api.model.TransferStream;
import com.piranha.storageapi.service.domain.BucketList;
import com.piranha.storageapi.service.domain.ObjectList;


/**
 * Handles requests for the application home page.
 */
@Controller
@RequestMapping(value = "/")
public class PiranhaStorageController {
	
	private static final Logger logger = LoggerFactory.getLogger(PiranhaStorageController.class);
	
	
	private AWSStorageServiceRepoImpl awsStorageServiceRepo;
	
	/**
	 * Simply selects the home view to render by returning its name.
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws FileNotFoundException 
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String home(Locale locale, Model model) throws FileNotFoundException, IllegalArgumentException, IOException {
		logger.info("Welcome to Piranha AWS S3 PoC on Spring and CF {}.", locale);
		
		Date date = new Date();
		DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG, locale);
		
		String formattedDate = dateFormat.format(date);
		
		model.addAttribute("PiranhaStorageController", formattedDate );
		
		awsStorageServiceRepo = new AWSStorageServiceRepoImpl(new File("WEB-INF/WSAwsCredentials.properties"));
		
		return "home";
	}
	
	@RequestMapping(value = "/PiranhaStorageService/buckets", method = RequestMethod.GET)
	
	public @ResponseBody BucketList handleListBuckets()
			throws PiranhaStroageException {
		ArrayList<String> buckets = new ArrayList<String>();

		buckets = awsStorageServiceRepo.listFolder();

		logger.info("Buckets count: " + buckets.size());

		BucketList bucketlist = new BucketList();
		bucketlist.setBuckets(buckets);
		
		return bucketlist;
	}
	
	
	@RequestMapping(value = "/PiranhaStorageService/buckets/{bucketName}", method = RequestMethod.GET)
	public @ResponseBody ObjectList  handleListObjectsInBucket(@PathVariable String bucketName) throws StorageFolderNotFound{
		ArrayList<String> objects = new ArrayList<String>();
		
		objects = awsStorageServiceRepo.listFileInFolder(bucketName);
		
		logger.info("Objects count: " + objects.size());
		logger.info("Bucket Name : " + bucketName);
		
		ObjectList objList = new ObjectList();
		
		objList.setBucketName(bucketName);
		objList.setObjects(objects);
		
		return objList;
	}
	//http://localhost:8080/storageapi/PiranhaStorageService/buckets/isites3/INGBTCPIC5NBA42_PicLFFile_635204726929544602.bin
	
	@RequestMapping(value = "/PiranhaStorageService/buckets/{bucketName}/{objectName:.+}", method = RequestMethod.GET)
	public void handleDownload(@PathVariable("bucketName") String bucketName, @PathVariable("objectName") String objectName, 
			HttpServletRequest request, HttpServletResponse response) throws IOException, PiranhaStroageException {
		
		String mimetype = request.getSession().getServletContext().getMimeType(objectName);
		TransferStream file = awsStorageServiceRepo.download(bucketName, objectName, 0, 0);
		response.setContentType(mimetype);
		response.setContentLength((int) file.getSize());
		response.setHeader("Content-Disposition","attachment; filename=\"" + objectName +"\"");
		FileCopyUtils.copy(file.getInputStream() , response.getOutputStream());

	}
	
	@RequestMapping(value = "/PiranhaStorageService/buckets/{bucketName}/{objectName:.+}/{from}/{to}", method = RequestMethod.GET)
	public void handleDownloadRange(@PathVariable("bucketName") String bucketName, @PathVariable("objectName") String objectName, 
			@PathVariable("from") String from, @PathVariable("to") String to,
			HttpServletRequest request, HttpServletResponse response) throws IOException, PiranhaStroageException, NumberFormatException {
		
		String mimetype = request.getSession().getServletContext().getMimeType(objectName);
		long lFrom = Long.parseLong(from, 10);
		long lto = Long.parseLong(to, 10);
		logger.info("From : " + lFrom);
		logger.info("To : "+ lto);
		TransferStream file = awsStorageServiceRepo.download(bucketName, objectName, lFrom, lto);
		response.setContentType(mimetype);
		response.setContentLength((int) file.getSize());
		response.setHeader("Content-Disposition","attachment; filename=\"" + objectName +"\"");
		FileCopyUtils.copy(file.getInputStream() , response.getOutputStream());

	}
	
	//Upload related methods
/*	@RequestMapping(value = "/PiranhaStorageService/buckets/{bucketName}/uploadform", method = RequestMethod.GET)
	public String  handleUploadForm(@PathVariable String bucketName, Model model) throws StroageFolderNotFound{		
		model.addAttribute("bucketName", bucketName);

		return "UploadForm";
	}*/
	@RequestMapping(value = "/PiranhaStorageService/buckets/{bucketName}/{objectName:.+}", method = RequestMethod.PUT)
	public void handleUpload(@PathVariable("bucketName") String bucketName, @PathVariable("objectName") String objectName,
			HttpServletRequest request, HttpServletResponse response) throws IOException, PiranhaStroageException{

        try {
            InputStream inputStream = request.getInputStream();
 
            if (inputStream != null) {
                String filePath = objectName;
                FileOutputStream outputStream = new FileOutputStream(new File(filePath));
 
                byte[] buffer = new byte[1024];
                int bytesRead;
                //awsStorageServiceRepo.store(bucketName, objectName, inputStream);
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                
                outputStream.flush();
                awsStorageServiceRepo.store(bucketName, objectName, inputStream);
                outputStream.close();
                
                FileInputStream fileUpload = new FileInputStream(filePath);
                awsStorageServiceRepo.store(bucketName, objectName, fileUpload);
              
                logger.info("Put file " + filePath);
            }
        } catch (FileNotFoundException e) {
        	logger.error(e.toString(), e);
        } catch (IOException e) {
        	logger.error(e.toString(), e);
        }
	}
	
	@ExceptionHandler({PiranhaStroageException.class, StorageFolderNotFound.class})
	ResponseEntity<String> handleAWSExceptions(Exception e)
	{
		return new ResponseEntity<String>(String.format("{reason: %s}", e.getMessage()), HttpStatus.NOT_FOUND);
	}
}
