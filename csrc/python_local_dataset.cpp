#include "dataset.h"
#include "local_trunk_controller.h"
#include "exceptions.h"
#include <dirent.h>
#include <Python.h>
#include "structmember.h"
#include<cstdarg>

typedef struct {
    PyObject_HEAD // no semicolon
    Dataset *dataset;
    LocalTrunkController *index;
    LocalTrunkController *data;
    int index_fd, data_fd;
    DIR* index_dir;
    DIR* data_dir;

} LocalDataset;

static void LocalDataset_dealloc(LocalDataset *self) {
    PyObject_GC_UnTrack(self);
    delete self->dataset;
    delete self->data;
    delete self->index;
    close(self->index_fd);
    close(self->data_fd);
    closedir(self->index_dir);
    closedir(self->data_dir);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *LocalDataset_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    LocalDataset *self;
    self = (LocalDataset *)type->tp_alloc(type, 0);
    return (PyObject *)self;
}

static int LocalDataset_init(LocalDataset *self, PyObject *args, PyObject *kwds) {
    char *basedir = NULL;
    int writable = 0;
    uint32_t trunk_size = 0;
    uint32_t trunks_per_file = 0;

    PyArg_ParseTuple(args, "spII", &basedir, &writable, &trunk_size, &trunks_per_file);
    char buffer[1024];
    int len_dir = strlen(basedir);
    if (basedir[len_dir - 1] == '/') {
        sprintf(buffer, "%sindex", basedir);
        self->index_dir = opendir(buffer);
        sprintf(buffer, "%sdata", basedir);
        self->data_dir = opendir(buffer);
    } else {   sprintf(buffer, "%s/index", basedir);
        self->index_dir = opendir(buffer);
        sprintf(buffer, "%s/data", basedir);
        self->data_dir = opendir(buffer);
    }
    if (self->index_dir == NULL || self->data_dir == NULL) {
        PyErr_SetString(PyExc_ValueError, "Failed to open dataset dir");
        return 0;
    }
    self->index_fd = dirfd(self->index_dir);
    self->data_fd = dirfd(self->data_dir);

    self->data = new LocalTrunkController(self->data_fd, !!writable, trunk_size, trunks_per_file);
    self->index = new LocalTrunkController(self->index_fd, !!writable, INDEX_FILE_SIZE, 1);
    return 0;
}

static PyObject* LocalDataset_write(LocalDataset *self, PyObject *args) {
    const char *data;
    uint32_t length;
    PyArg_ParseTuple(args, "yI", &data, &length);
    try {
        self->dataset->write(DataView(data, length));
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return Py_BuildValue("");
}

static PyObject* LocalDataset_flush(LocalDataset *self, PyObject *Py_UNUSED(args)) {
    try {
        self->dataset->flush();
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return Py_BuildValue("");
}

static PyObject *LocalDataset_read(LocalDataset *self, PyObject *Py_UNUSED(args)) {
    PyObject *ret = NULL;
    try {
        DataView v = self->dataset->read();
        ret = PyBytes_FromStringAndSize(v.data, v.length);
        v.free();
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return ret;
}

static PyObject* LocalDataset_seek(LocalDataset *self, PyObject *args) {
    uint32_t offset, whence;
    PyArg_ParseTuple(args, "II", &offset, &whence);
    try {
        self->dataset->seek(offset, whence);
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return Py_BuildValue("");
}

static PyObject *LocalDataset_pread(LocalDataset *self, PyObject *args) {
    uint32_t offset;
    PyArg_ParseTuple(args, "I", &offset);
    PyObject *ret = NULL;
    try {
        DataView v = self->dataset->pread(offset);
        ret = PyBytes_FromStringAndSize(v.data, v.length);
        v.free();
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return ret;
}

static PyObject* LocalDataset_size(LocalDataset *self, PyObject *Py_UNUSED(args)) {
    try {
        return Py_BuildValue("I", self->dataset->size());
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return NULL;
}

static PyObject* LocalDataset_tell(LocalDataset *self, PyObject *Py_UNUSED(args)) {
    try {
        return Py_BuildValue("I", self->dataset->tell());
    } catch (KaraStorageException e) {
        PyErr_SetString(PyExc_RuntimeError, e.msg.c_str());
    }
    return NULL;
}

static PyMemberDef LocalDataset_members[] = {
    {NULL, 0, 0, 0, NULL}
};

static PyMethodDef LocalDataset_methods[] = {
    {"write", (PyCFunction)LocalDataset_write, METH_VARARGS, "append data to dataset"},
    {"read", (PyCFunction)LocalDataset_read, METH_VARARGS, "read from dataset"},
    {"flush", (PyCFunction)LocalDataset_flush, METH_VARARGS, "flush buffer"},
    {"pread", (PyCFunction)LocalDataset_pread, METH_VARARGS, "pread from dataset"},
    {"seek", (PyCFunction)LocalDataset_seek, METH_VARARGS, "seek"},
    {"size", (PyCFunction)LocalDataset_size, METH_VARARGS, "size"},
    {"tell", (PyCFunction)LocalDataset_tell, METH_VARARGS, "tell"},
    {NULL}  /* Sentinel */
};

static PyTypeObject LocalDatasetType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "LocalDataset",             /* tp_name */
    sizeof(LocalDataset),             /* tp_basicsize */
    0,                         /* tp_itemsize */
    (destructor)LocalDataset_dealloc, /* tp_dealloc */
    0,                         /* tp_print */
    0,                         /* tp_getattr */
    0,                         /* tp_setattr */
    0,                         /* tp_reserved */
    0,                         /* tp_repr */
    0,                         /* tp_as_number */
    0,                         /* tp_as_sequence */
    0,                         /* tp_as_mapping */
    0,                         /* tp_hash  */
    0,                         /* tp_call */
    0,                         /* tp_str */
    0,                         /* tp_getattro */
    0,                         /* tp_setattro */
    0,                         /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT |
        Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "LocalDataset c wrapper",           /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    LocalDataset_methods,             /* tp_methods */
    LocalDataset_members,             /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)LocalDataset_init,      /* tp_init */
    0,                         /* tp_alloc */
    LocalDataset_new,                 /* tp_new */
};


static struct PyModuleDef local_dataset_module = {
    PyModuleDef_HEAD_INIT,
    "local_dataset",
    "kara local storage c interface",
    -1,
    NULL, NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit_local_dataset(void)
{
    PyObject* m;

    if (PyType_Ready(&LocalDatasetType) < 0)
        return NULL;

    m = PyModule_Create(&local_dataset_module);
    if (m == NULL)
        return NULL;

    Py_INCREF(&LocalDatasetType);
    if (PyModule_AddObject(m, "LocalDataset", (PyObject *)&LocalDatasetType) < 0) {
        Py_DECREF(&LocalDatasetType);
        Py_DECREF(m);
        return NULL;
    }
    return m;
}


