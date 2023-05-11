package slidewindows.wtp;

import com.google.common.base.CaseFormat;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MyEntityClassFactory {

    private static Map<String, Class<?>> classMap = new HashMap<>();

    public static Map<String, Class<?>> init() throws IOException, ClassNotFoundException {
        Class<MyEntityClassFactory> factoryClass = MyEntityClassFactory.class;
        Package factoryClassPackage = factoryClass.getPackage();
        System.out.println("factoryClassPackage = " + factoryClassPackage);
        String factoryClassPackageName = factoryClassPackage.getName();

        System.out.println("factoryClassPackageName = " + factoryClassPackageName);
        String domainPackageName = factoryClassPackageName + ".domain";

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        String domainPackagePath = domainPackageName.replaceAll("\\.", File.separator);
        Enumeration<URL> resources = contextClassLoader.getResources(domainPackagePath);

        List<Class<?>> classList = new ArrayList<>();

        List<File> fileList = new ArrayList<>();
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            if (url.getProtocol().equals("file")) {
                listFiles(new File(url.getFile()), fileList);
                classList.addAll(loadClasses(fileList, domainPackageName));
            }
        }
        for (Class<?> aClass : classList) {
            classMap.put(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, aClass.getSimpleName()), aClass);
        }

        return classMap;

    }

    private static List<Class<?>> loadClasses(List<File> classes, String scan) throws ClassNotFoundException {
        List<Class<?>> clazzes = new ArrayList<Class<?>>();
        for (File file : classes) {
            String fPath = file.getAbsolutePath().replaceAll(File.separator, ".");
            // 把 包路径 前面的 盘符等 去掉 ， 这里必须是lastIndexOf ，防止名称有重复的
            String packageName = fPath.substring(fPath.lastIndexOf(scan));
            // 去掉后缀.class ，并且把 / 替换成 .    这样就是  com.hadluo.A 格式了 ， 就可以用Class.forName加载了
            packageName = packageName.replace(".class", "").replaceAll(File.separator, ".");
            // 根据名称加载类
            clazzes.add(Class.forName(packageName));
        }
        return clazzes;

    }

    private static void listFiles(File dir, List<File> fileList) {
        if (dir.isDirectory()) {
            for (File f : Objects.requireNonNull(dir.listFiles())) {
                listFiles(f, fileList);
            }
        } else {
            if (dir.getName().endsWith(".class")) {
                fileList.add(dir);
            }
        }
    }

}
